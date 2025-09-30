# File: pipelines/line_crossings_autoloader_fixed.py
"""
Script to load line crossing JSON events into a Delta table using Databricks Auto Loader.

Features:
- Automatically creates catalog.schema.table if not present.
- Ensures required `id` column exists and is derived from `alert_id`.
- Converts nanosecond epoch timestamps to TIMESTAMP correctly.
- Produces consistent relative URLs for image and video paths.
- Reads optional binary blobs (video/image) via UDFs with retry handling.
- Uses foreachBatch + MERGE for idempotent upserts into Delta.
- Separates Auto Loader schema location from the streaming checkpoint.
"""

from __future__ import annotations

import os
import re
import time
from typing import Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# ------------------------------
# Parameters (Databricks widgets)
# ------------------------------
try:
    dbutils  # type: ignore[name-defined]
except NameError:
    class _Dummy:
        class widgets:
            @staticmethod
            def text(name: str, default: str = "", label: str | None = None) -> None:
                pass

            @staticmethod
            def get(name: str) -> str:
                return os.environ.get(name, "")

    dbutils = _Dummy()  # type: ignore

# Inputs
dbutils.widgets.text("input_path", "s3://safepointe-ml-int/event-data", label="Input Volume Path")
dbutils.widgets.text("output_table_name", "dev.safepointe.line_crossings", label="Output Table Name")
dbutils.widgets.text("checkpoint_location", "/Volumes/dev/safepointe/_checkpoints/line_crossings", label="Checkpoint Location")
dbutils.widgets.text("video_volume_path", "/Volumes/dev/safepointe/event-data", label="Video Volume Path")
dbutils.widgets.text("image_volume_path", "/Volumes/dev/safepointe/alert-data", label="Image Volume Path")

input_path = dbutils.widgets.get("input_path")
output_table_name = dbutils.widgets.get("output_table_name")
checkpoint_location = dbutils.widgets.get("checkpoint_location")
video_volume_path = dbutils.widgets.get("video_volume_path")
image_volume_path = dbutils.widgets.get("image_volume_path")

if "prod" in (video_volume_path or ""):
    video_volume_path = "/Volumes/safepointe-ml-catalog/default/safepointe-ml-prd/event-data"

assert input_path, "input_path notebook parameter must be specified"
assert output_table_name, "output_table_name notebook parameter must be specified"
assert checkpoint_location, "checkpoint_location notebook parameter must be specified"
assert video_volume_path, "video_volume_path notebook parameter must be specified"
assert image_volume_path, "image_volume_path notebook parameter must be specified"

schema_location = os.path.join(checkpoint_location.rstrip("/"), "_autoloader_schema")

# ------------------------------
# Helpers for 3-part table names
# ------------------------------

def parse_uc_name(three_part: str) -> tuple[str, str, str]:
    parts = three_part.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        current_catalog = spark.sql("SELECT current_catalog() AS c").first()["c"]
        return current_catalog, parts[0], parts[1]
    if len(parts) == 1:
        current_catalog = spark.sql("SELECT current_catalog() AS c").first()["c"]
        current_schema = spark.sql("SELECT current_schema()  AS s").first()["s"]
        return current_catalog, current_schema, parts[0]
    raise ValueError(f"Unexpected object name: {three_part}")

catalog, schema, table = parse_uc_name(output_table_name)
full_name = f"{catalog}.{schema}.{table}"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {catalog}.{schema}")

# ------------------------------
# Destination table (Delta)
# ------------------------------
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {full_name} (
        id STRING NOT NULL,
        path STRING,
        ts TIMESTAMP,
        video_rel_url STRING,
        video_blob BINARY,
        image_rel_url STRING,
        image_blob BINARY
    )
    USING DELTA
    CLUSTER BY (id, ts)
    """
)

schema_evolution_mode = "addNewColumns"

# ------------------------------
# Path mapping + binary readers
# ------------------------------

def _derive_video_path(json_s3_path: str) -> Optional[str]:
    if not json_s3_path:
        return None
    rel = json_s3_path.split("event-data/")[-1]
    rel = re.sub(r"(_crossing_event|_data_package)\\.json$", "_tm2video.mp4", rel)
    rel = re.sub(r"^(.*?[0-9]+_[0-9]+)_[0-9]+(_tm2video\\.mp4)$", r"\1\2", rel)
    return rel


def _derive_image_path(json_s3_path: str) -> Optional[str]:
    if not json_s3_path:
        return None
    rel = json_s3_path.split("event-data/")[-1]
    rel = re.sub(r"(_crossing_event|_data_package)\\.json$", "_tm2image.jpg", rel)
    rel = re.sub(r"^(.*?[0-9]+_[0-9]+)_[0-9]+(_tm2image\\.jpg)$", r"\1\2", rel)
    return rel


def _read_bytes(base: str, rel: Optional[str], *, max_retries: int = 3, retry_delay: float = 0.5) -> Tuple[Optional[str], Optional[bytes]]:
    if not rel:
        return None, None
    full = os.path.join(base, rel)
    for attempt in range(max_retries):
        try:
            with open(full, "rb") as fh:
                return rel, fh.read()
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return None, None
    return None, None


def get_video(json_path: Optional[str]) -> Tuple[Optional[str], Optional[bytes]]:
    rel = _derive_video_path(json_path or "")
    rel_url, blob = _read_bytes(video_volume_path, rel)
    return (f"event-data/{rel_url}" if rel_url else None, blob)


def get_alert_image(json_path: Optional[str]) -> Tuple[Optional[str], Optional[bytes]]:
    rel = _derive_image_path(json_path or "")
    rel_url, blob = _read_bytes(image_volume_path, rel)
    return (f"alert-data/{rel_url}" if rel_url else None, blob)

get_video_udf = F.udf(
    get_video,
    StructType([
        StructField("video_rel_url", StringType(), True),
        StructField("video_blob", BinaryType(), True),
    ]),
)

get_image_udf = F.udf(
    get_alert_image,
    StructType([
        StructField("image_rel_url", StringType(), True),
        StructField("image_blob", BinaryType(), True),
    ]),
)

# ------------------------------
# Build streaming DataFrame
# ------------------------------

def load_line_crossings() -> "DataFrame":
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
        .option("pathGlobFilter", "*.json")
        .load(input_path)
    )

    df = (
        df.withColumn("id", F.col("alert_id").cast("string"))
          .withColumn("sensorlocation_id", F.col("sensorlocation_id").cast("bigint"))
    )

    ts_seconds = (F.col("timestamp").cast("double") / F.lit(1_000_000_000))
    df = df.withColumn("ts", F.to_timestamp(F.from_unixtime(ts_seconds)))

    df = df.withColumn("path", F.col("_metadata.file_path"))

    video_struct = get_video_udf(F.col("path"))
    image_struct = get_image_udf(F.col("path"))

    df = (
        df.withColumn("video_rel_url", video_struct["video_rel_url"])
          .withColumn("video_blob",    video_struct["video_blob"])
          .withColumn("image_rel_url", image_struct["image_rel_url"])
          .withColumn("image_blob",    image_struct["image_blob"])
    )

    return df.select("id", "path", "ts", "video_rel_url", "video_blob", "image_rel_url", "image_blob")


line_crossings_df = load_line_crossings()

# ------------------------------
# Upsert (foreachBatch + MERGE)
# ------------------------------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


def upsert_to_delta(batch_df, batch_id: int) -> None:
    batch_df.createOrReplaceTempView("updates")
    spark.sql(
        f"""
        MERGE INTO {full_name} AS target
        USING updates AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


query = (
    line_crossings_df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", checkpoint_location)
    .option("maxFilesPerTrigger", "10000")
    .option("maxBytesPerTrigger", "10g")
    .trigger(availableNow=True)
    .start()
)

try:
    query.awaitTermination()
except Exception:
    pass
