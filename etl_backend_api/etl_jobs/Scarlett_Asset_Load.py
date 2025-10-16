#!/usr/bin/env python3
"""
Scarlett Asset Load ETL (PySpark)

Reads a CSV with specified headers, deduplicates by 'Title ID', maps fields to the Assets schema,
adds UTC timestamps with millisecond precision, and writes to PostgreSQL using JDBC.

Configuration via environment variables (.env supported via python-dotenv if present):
- ASSET_CSV_PATH: Path to input CSV (required)
- PG_JDBC_URL: JDBC URL in the form jdbc:postgresql://host:port/dbname
- PG_USER: PostgreSQL username
- PG_PASSWORD: PostgreSQL password
- PG_SCHEMA: Target schema (default: public)
- PG_TABLE: Target table (default: Assets)
- PREVIEW_ONLY: If 'true', show 20 rows and exit before DB write

CSV read options:
- header=True, inferSchema=False, explicit schema for the expected columns
- multiLine=True, mode=PERMISSIVE, quote='"', escape='"'

Transformations:
- Drop null/empty 'Title ID', then dropDuplicates(['Title ID'])
- Map to Assets output fields:
  asset_id: null
  asset_type_id: 68
  asset_source_id: 'Titles'
  business_entity_id: 'NA'
  migrated_rms_id: 3
  asset_name: from 'Title Name'
  asset_group_id: 1
  episode_number: 0
  release_year: null (int)
  created_on, last_modified_on, imported_on: current UTC 'YYYY-MM-DD HH:MM:SS.mmm'
  is_active: true
- Optional debug fields maintained transiently: title_id, series_name, title_type
- Apply string truncation safeguard to 10000 chars (truncate to 9999 if > 10000)

Usage:
- Preview:
    PREVIEW_ONLY=true ASSET_CSV_PATH=/path/to/SampleDump.csv \\
    spark-submit etl_backend_api/etl_jobs/Scarlett_Asset_Load.py

- Write (append):
    ASSET_CSV_PATH=/path/to/SampleDump.csv \\
    PG_JDBC_URL="jdbc:postgresql://localhost:5432/rightsdb" PG_USER=myuser PG_PASSWORD=mypass \\
    PG_SCHEMA=public PG_TABLE=Assets \\
    spark-submit --packages org.postgresql:postgresql:42.7.4 etl_backend_api/etl_jobs/Scarlett_Asset_Load.py
"""
import os
import sys
from datetime import datetime, timezone

try:
    # Load .env if available; ignore if python-dotenv isn't installed
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _get_env_bool(name: str, default: bool = False) -> bool:
    """Return boolean from env var with common truthy values."""
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y")


def _current_utc_ms_string() -> str:
    """Return current UTC time as string 'YYYY-MM-DD HH:MM:SS.mmm'."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(now.microsecond / 1000):03d}"


def _build_spark() -> SparkSession:
    """Create SparkSession with UTC timezone and local master for standalone runs."""
    spark = (
        SparkSession.builder.appName("ScarlettAssetLoad")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _csv_schema() -> T.StructType:
    """Explicit CSV schema using StringType unless numeric is obvious."""
    return T.StructType(
        [
            T.StructField("Title ID", T.StringType(), True),
            T.StructField("Title Name", T.StringType(), True),
            T.StructField("Availability Start Date", T.StringType(), True),
            T.StructField("Availability End Date", T.StringType(), True),
            T.StructField("Exhibitions Indicator", T.StringType(), True),
            # obvious numeric
            T.StructField("No of Plays", T.IntegerType(), True),
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


def _read_csv(spark: SparkSession, path: str):
    """Read CSV with robust options and explicit schema."""
    print(f"[INFO] Reading CSV from path: {path}")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .schema(_csv_schema())
        .csv(path)
    )
    print(f"[INFO] Loaded input rows: {df.count()}")
    print(f"[INFO] Input columns: {df.columns}")
    return df


def _dedupe_by_title_id(df):
    """Trim, filter non-empty Title ID and dedupe."""
    cleaned = df.withColumn("Title ID", F.trim(F.col("Title ID")))
    filtered = cleaned.filter(F.col("Title ID").isNotNull() & (F.length(F.col("Title ID")) > 0))
    distinct_count = filtered.select("Title ID").distinct().count()
    deduped = filtered.dropDuplicates(["Title ID"])
    print(f"[INFO] Distinct Title ID count: {distinct_count}")
    print(f"[INFO] Rows after Title ID filtering & de-dup: {deduped.count()}")
    return deduped


def _map_to_assets(df):
    """Map the input df to the Assets schema with timestamps and default values."""
    ts = _current_utc_ms_string()

    mapped = (
        df.select(
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
        # optional/debug lineage fields (not written if destination schema lacks them)
        "title_id",
        "series_name",
        "title_type",
    ]
    # Ensure any missing selected columns exist as String
    for c in ["title_id", "asset_name", "series_name", "title_type"]:
        if c not in mapped.columns:
            mapped = mapped.withColumn(c, F.lit(None).cast(T.StringType()))

    return mapped.select(*final_cols)


def _truncate_string_columns(df):
    """Apply truncation safeguard: if string length > 10000, truncate to 9999."""
    MAX_ALLOWED = 10000
    TRIM_LENGTH = 9999
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            df = df.withColumn(
                field.name,
                F.when(F.length(F.col(field.name)) > MAX_ALLOWED, F.col(field.name).substr(1, TRIM_LENGTH)).otherwise(
                    F.col(field.name)
                ),
            )
    return df


def _write_jdbc(final_df):
    """Write DataFrame to PostgreSQL via JDBC in append mode with options."""
    # PUBLIC_INTERFACE
    def _validate_env():
        """Validate and construct JDBC options from environment variables."""
        jdbc_url = os.getenv("PG_JDBC_URL")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")
        schema = os.getenv("PG_SCHEMA", "public")
        table = os.getenv("PG_TABLE", "Assets")

        missing = [k for k, v in {"PG_JDBC_URL": jdbc_url, "PG_USER": user, "PG_PASSWORD": password}.items() if not v]
        if missing:
            raise RuntimeError(
                f"Missing required DB env vars: {', '.join(missing)}. "
                "Set PG_JDBC_URL, PG_USER, PG_PASSWORD or run with PREVIEW_ONLY=true."
            )

        dbtable = f"{schema}.{table}" if schema else table
        return jdbc_url, user, password, dbtable

    jdbc_url, user, password, dbtable = _validate_env()

    print(
        f"[INFO] Writing to PostgreSQL via JDBC: url={jdbc_url}, table={dbtable}, mode=append, "
        "driver=org.postgresql.Driver, batchsize=5000, isolationLevel=READ_COMMITTED"
    )
    try:
        (
            final_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", dbtable)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "5000")
            .option("isolationLevel", "READ_COMMITTED")
            .mode("append")
            .save()
        )
        print("[INFO] JDBC write completed successfully.")
    except Exception as exc:
        print(f"[ERROR] JDBC write failed: {exc}")
        raise


# PUBLIC_INTERFACE
def main():
    """Run the Scarlett Asset Load ETL job."""
    csv_path = os.getenv("ASSET_CSV_PATH")
    if not csv_path or not str(csv_path).strip():
        print("[ERROR] Required environment variable ASSET_CSV_PATH is missing or empty.")
        print("Set ASSET_CSV_PATH to the input CSV file path.")
        sys.exit(2)

    preview_only = _get_env_bool("PREVIEW_ONLY", False)

    spark = _build_spark()
    try:
        df = _read_csv(spark, csv_path)
        df_dedup = _dedupe_by_title_id(df)
        final_df = _map_to_assets(df_dedup)
        final_df = _truncate_string_columns(final_df)

        print("[INFO] Final schema:")
        final_df.printSchema()
        count_out = final_df.count()
        print(f"[INFO] Final row count: {count_out}")

        if preview_only:
            print("[INFO] PREVIEW_ONLY=true -> showing 20 rows and exiting without DB write.")
            final_df.show(20, truncate=False)
            sys.exit(0)

        # Attempt JDBC write
        try:
            _write_jdbc(final_df)
        except Exception:
            sys.exit(1)

    finally:
        spark.stop()
        print("[INFO] Scarlett Asset Load job finished.")


if __name__ == "__main__":
    main()
