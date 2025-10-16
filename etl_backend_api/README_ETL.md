# Assets ETL (PySpark)

This repository includes a standalone PySpark ETL script to ingest an input CSV and load mapped Asset records into PostgreSQL.

## Script

- Path: `etl_backend_api/etl_jobs/assets_etl_job.py`
- Engine: PySpark (no AWS Glue dependency)
- Modes: Preview and Full Load

## Environment Variables

Required for reading:
- ASSET_CSV_PATH: Path to the CSV file to ingest.

Preview control:
- PREVIEW: Set to `true` to preview top 20 rows and exit without writing to DB.

PostgreSQL connection (required when PREVIEW is not true):
- PG_HOST: PostgreSQL hostname
- PG_PORT: PostgreSQL port (e.g., 5432)
- PG_DB: Database name
- PG_USER: Username
- PG_PASSWORD: Password
- PG_SCHEMA: Optional; default `public`
- PG_TABLE: Optional; default `Assets`

Performance tuning:
- BATCH_SIZE: Optional; default `1000`
- PARTITIONS: Optional; if set to an integer > 0, repartitions the DataFrame before JDBC write

## CSV Input Schema

Expected headers (all read as StringType):
- Title ID, Title Name, Availability Start Date, Availability End Date, Exhibitions Indicator, No of Plays, Time frame, Allowed Plays per Timeframe, Exhibitions Allowed, Exhibitions Scheduled, Exhibitions Taken, Exhibitions Transferred, Exhibitions Remaining, Telecast Allowed, Telecast Scheduled, Telecast Taken, Telecast Transferred, Telecast Remaining, Unlimited Runs, Network CD, Contract ID, Contract Name, Package ID, Package Name, Contract Start Date, Contract End Date, Startover Rights, Contract Create Date, Distributor Name, Acquisition Method, C.A.R, Languages, Series Name, Title Type

## Transformations

- Trim and filter non-empty `Title ID`
- Deduplicate by `Title ID`
- Map to Asset fields:
  - asset_id: null (Integer)
  - asset_type_id: 68
  - asset_source_id: 'Titles'
  - business_entity_id: 'NA'
  - migrated_rms_id: 3
  - asset_name: from `Title Name`
  - asset_group_id: 1
  - episode_number: 0
  - release_year: null
  - created_on, last_modified_on, imported_on: current UTC timestamp formatted as `YYYY-MM-DD HH:MM:SS.SSS`
  - is_active: true
  - title_id: from `Title ID`
  - series_name: from `Series Name`
  - title_type: from `Title Type`
- String columns are truncated to at most 10000 characters.

## Running

Install Spark and provide the PostgreSQL JDBC driver.

- Preview mode (no DB write):
```
PREVIEW=true ASSET_CSV_PATH=/absolute/path/to/SampleDump.csv \
spark-submit etl_backend_api/etl_jobs/assets_etl_job.py
```

- Full load (append to PostgreSQL table):
```
ASSET_CSV_PATH=/absolute/path/to/SampleDump.csv \
PG_HOST=localhost PG_PORT=5432 PG_DB=mydb PG_USER=myuser PG_PASSWORD=mypass \
PG_SCHEMA=public PG_TABLE=Assets BATCH_SIZE=1000 PARTITIONS=4 \
spark-submit --packages org.postgresql:postgresql:42.7.4 etl_backend_api/etl_jobs/assets_etl_job.py
```

## Notes

- The job prints input row counts, distinct Title ID count, output count, and the final schema.
- The write is performed in `append` mode.
- Ensure the destination table exists (schema validation is not performed by this script).
