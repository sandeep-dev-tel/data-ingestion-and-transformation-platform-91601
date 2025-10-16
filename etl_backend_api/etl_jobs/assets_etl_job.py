#!/usr/bin/env python3
"""
Assets ETL Job (PySpark)

Reads a CSV with specific columns, deduplicates by Title ID, maps to the Assets table schema
with default/fixed values and timestamps, and writes to PostgreSQL using JDBC.

Configuration via environment variables:
- ASSET_CSV_PATH: Path to input CSV (required)
- PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD: PostgreSQL connection (required when not in preview)
- PG_SCHEMA (optional; default: public)
- PG_TABLE (optional; default: Assets)
- PREVIEW (optional; 'true' to preview 20 rows and exit)
- BATCH_SIZE (optional; default: 1000)
- PARTITIONS (optional; if set to int > 0, repartition output before write)

Usage examples:
- Preview mode:
    PREVIEW=true ASSET_CSV_PATH=/path/to/file.csv spark-submit etl_backend_api/etl_jobs/assets_etl_job.py
- Full load:
    ASSET_CSV_PATH=/path/to/file.csv PG_HOST=... PG_PORT=5432 PG_DB=... PG_USER=... PG_PASSWORD=... && \
    spark-submit --packages org.postgresql:postgresql:42.7.4 etl_backend_api/etl_jobs/assets_etl_job.py
"""
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _get_env_bool(name: str, default: bool = False) -> bool:
    """Return boolean from env var with common truthy values."""
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y")


# PUBLIC_INTERFACE
def main():
    """Entry point: runs the Assets ETL job per env configuration."""
    # Validate CSV path
    csv_path = os.getenv("ASSET_CSV_PATH")
    if not csv_path or not str(csv_path).strip():
        print("[ERROR] Required environment variable ASSET_CSV_PATH is missing or empty.")
        print("Set ASSET_CSV_PATH to the input CSV file path.")
        sys.exit(2)

    preview = _get_env_bool("PREVIEW", False)

    # Build SparkSession
    spark = (
        SparkSession.builder.appName("AssetsETLJob")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Reading CSV from path: {csv_path}")

    # Define schema: all strings unless we cast later
    csv_schema = T.StructType(
        [
            T.StructField("Title ID", T.StringType(), True),
            T.StructField("Title Name", T.StringType(), True),
            T.StructField("Availability Start Date", T.StringType(), True),
            T.StructField("Availability End Date", T.StringType(), True),
            T.StructField("Exhibitions Indicator", T.StringType(), True),
            T.StructField("No of Plays", T.StringType(), True),
            T.StructField("Time frame", T.StringType(), True),
            T.StructField("Allowed Plays per Timeframe", T.StringType(), True),
            T.StructField("Exhibitions Allowed", T.StringType(), True),
            T.StructField("Exhibitions Scheduled", T.StringType(), True),
            T.StructField("Exhibitions Taken", T.StringType(), True),
            T.StructField("Exhibitions Transferred", T.StringType(), True),
            T.StructField("Exhibitions Remaining", T.StringType(), True),
            T.StructField("Telecast Allowed", T.StringType(), True),
            T.StructField("Telecast Scheduled", T.StringType(), True),
            T.StructField("Telecast Taken", T.StringType(), True),
            T.StructField("Telecast Transferred", T.StringType(), True),
            T.StructField("Telecast Remaining", T.StringType(), True),
            T.StructField("Unlimited Runs", T.StringType(), True),
            T.StructField("Network CD", T.StringType(), True),
            T.StructField("Contract ID", T.StringType(), True),
            T.StructField("Contract Name", T.StringType(), True),
            T.StructField("Package ID", T.StringType(), True),
            T.StructField("Package Name", T.StringType(), True),
            T.StructField("Contract Start Date", T.StringType(), True),
            T.StructField("Contract End Date", T.StringType(), True),
            T.StructField("Startover Rights", T.StringType(), True),
            T.StructField("Contract Create Date", T.StringType(), True),
            T.StructField("Distributor Name", T.StringType(), True),
            T.StructField("Acquisition Method", T.StringType(), True),
            T.StructField("C.A.R", T.StringType(), True),
            T.StructField("Languages", T.StringType(), True),
            T.StructField("Series Name", T.StringType(), True),
            T.StructField("Title Type", T.StringType(), True),
        ]
    )

    # Read CSV with robust options
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .schema(csv_schema)
        .csv(csv_path)
    )

    input_count = df.count()
    print(f"[INFO] Loaded input rows: {input_count}")
    print(f"[INFO] Input columns: {df.columns}")

    # Trim Title ID, filter non-empty, deduplicate by Title ID
    df_clean = df.withColumn("Title ID", F.trim(F.col("Title ID")))
    df_filtered = df_clean.filter(F.col("Title ID").isNotNull() & (F.length(F.col("Title ID")) > 0))
    distinct_count = df_filtered.select("Title ID").distinct().count()
    df_dedup = df_filtered.dropDuplicates(["Title ID"])

    print(f"[INFO] Distinct Title ID count: {distinct_count}")
    print(f"[INFO] Rows after Title ID filtering & de-dup: {df_dedup.count()}")

    # Prepare timestamps: UTC with millisecond precision as string
    now = datetime.utcnow()
    ts = now.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(now.microsecond / 1000):03d}"

    # Map to destination Assets schema
    # Columns required per mapping instructions
    final_df = (
        df_dedup.select(
            F.col("Title ID").alias("title_id"),
            F.col("Title Name").alias("asset_name"),
            F.col("Series Name").alias("series_name"),
            F.col("Title Type").alias("title_type"),
        )
        .withColumn("asset_id", F.lit(None).cast(T.IntegerType()))
        .withColumn("asset_type_id", F.lit(68).cast(T.IntegerType()))
        .withColumn("asset_source_id", F.lit("Titles"))
        .withColumn("business_entity_id", F.lit("NA"))
        .withColumn("migrated_rms_id", F.lit(3).cast(T.IntegerType()))
        .withColumn("asset_group_id", F.lit(1).cast(T.IntegerType()))
        .withColumn("episode_number", F.lit(0).cast(T.IntegerType()))
        .withColumn("release_year", F.lit(None).cast(T.IntegerType()))
        .withColumn("created_on", F.lit(ts))
        .withColumn("last_modified_on", F.lit(ts))
        .withColumn("imported_on", F.lit(ts))
        .withColumn("is_active", F.lit(True))
    )

    # Reorder columns exactly as specified
    final_cols = [
        "asset_id",
        "asset_type_id",
        "asset_source_id",
        "business_entity_id",
        "migrated_rms_id",
        "asset_name",
        "asset_group_id",
        "episode_number",
        "release_year",
        "created_on",
        "last_modified_on",
        "imported_on",
        "is_active",
        # Optional informational columns aligned with mapping inputs
        "title_id",
        "series_name",
        "title_type",
    ]
    # Ensure all expected columns exist (in case input lacks some)
    for c in ["title_id", "asset_name", "series_name", "title_type"]:
        if c not in final_df.columns:
            final_df = final_df.withColumn(c, F.lit(None).cast(T.StringType()))

    final_df = final_df.select(*final_cols)

    # Truncate string columns safeguard (to avoid JDBC issues)
    MAX_ALLOWED = 10000
    TRIM_LENGTH = 10000  # store at most 10k
    for field in final_df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            final_df = final_df.withColumn(
                field.name,
                F.when(F.length(F.col(field.name)) > MAX_ALLOWED, F.expr(f"substring({field.name}, 1, {TRIM_LENGTH})"))
                .otherwise(F.col(field.name)),
            )

    print("[INFO] Final schema:")
    final_df.printSchema()
    out_count = final_df.count()
    print(f"[INFO] Final row count: {out_count}")

    # Preview mode
    if preview:
        print("[INFO] Preview mode enabled; displaying top 20 rows and exiting without DB write.")
        final_df.show(20, truncate=False)
        spark.stop()
        sys.exit(0)

    # JDBC configuration
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT")
    pg_db = os.getenv("PG_DB")
    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")
    pg_schema = os.getenv("PG_SCHEMA", "public")
    pg_table = os.getenv("PG_TABLE", "Assets")
    batch_size = int(os.getenv("BATCH_SIZE", "1000"))
    partitions = os.getenv("PARTITIONS")

    # Validate DB env
    missing = [k for k, v in {
        "PG_HOST": pg_host, "PG_PORT": pg_port, "PG_DB": pg_db,
        "PG_USER": pg_user, "PG_PASSWORD": pg_password
    }.items() if not v]
    if missing:
        print(f"[ERROR] Missing required DB environment variables for write: {', '.join(missing)}")
        print("Set PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD or run with PREVIEW=true.")
        spark.stop()
        sys.exit(2)

    try:
        pg_port_int = int(pg_port)
    except Exception:
        print("[ERROR] PG_PORT must be an integer.")
        spark.stop()
        sys.exit(2)

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port_int}/{pg_db}"
    dbtable = f"{pg_schema}.{pg_table}" if pg_schema else pg_table
    props = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver",
        "batchsize": str(batch_size),
    }

    # Optional repartition before write to manage parallelism
    df_to_write = final_df
    if partitions:
        try:
            n = int(partitions)
            if n > 0:
                print(f"[INFO] Repartitioning output to {n} partitions for JDBC write.")
                df_to_write = final_df.repartition(n)
        except Exception:
            print(f"[WARN] Invalid PARTITIONS={partitions}; ignoring.")

    print(f"[INFO] Writing to PostgreSQL: url={jdbc_url}, table={dbtable}, mode=append, batchsize={batch_size}")
    df_to_write.write.jdbc(url=jdbc_url, table=dbtable, mode="append", properties=props)
    print("[INFO] JDBC write completed successfully.")

    spark.stop()
    print("[INFO] Assets ETL job finished.")


if __name__ == "__main__":
    main()
