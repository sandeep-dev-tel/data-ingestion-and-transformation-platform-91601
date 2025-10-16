# Scarlett Asset Load (PySpark)

A standalone PySpark ETL script that reads a CSV, deduplicates by `Title ID`, maps fields to the Assets schema, timestamps rows with UTC milliseconds, and writes to PostgreSQL via JDBC. Supports a preview mode to inspect the final dataset before writing.

## Script

- Path: `etl_backend_api/etl_jobs/Scarlett_Asset_Load.py`
- Engine: PySpark (runs locally with `local[*]`)
- Modes: Preview and Write (append)

## Environment Variables

Required input:
- ASSET_CSV_PATH: Absolute or relative path to the CSV file

Preview control:
- PREVIEW_ONLY: Set to `true` to preview top 20 rows and exit without writing to DB

PostgreSQL connection (required when PREVIEW_ONLY is not true):
- PG_JDBC_URL: JDBC URL (e.g., `jdbc:postgresql://localhost:5432/rightsdb`)
- PG_USER: Username
- PG_PASSWORD: Password
- PG_SCHEMA: Optional; default `public`
- PG_TABLE: Optional; default `Assets`

## CSV Input Schema

Expected headers:
- Title ID, Title Name, Availability Start Date, Availability End Date, Exhibitions Indicator, No of Plays, Time frame, Allowed Plays per Timeframe, Exhibitions Allowed, Exhibitions Scheduled, Exhibitions Taken, Exhibitions Transferred, Exhibitions Remaining, Telecast Allowed, Telecast Scheduled, Telecast Taken, Telecast Transferred, Telecast Remaining, Unlimited Runs, Network CD, Contract ID, Contract Name, Package ID, Package Name, Contract Start Date, Contract End Date, Startover Rights, Contract Create Date, Distributor Name, Acquisition Method, C.A.R, Languages, Series Name, Title Type

Notes:
- The job uses an explicit schema and reads almost all columns as StringType; `No of Plays` as IntegerType.
- Robust CSV parsing: `multiLine=True`, `mode=PERMISSIVE`, `quote="`, `escape="`.

## Transformations

- Trim and filter non-empty `Title ID`, de-duplicate by `Title ID`.
- Map to Asset fields:
  - asset_id: null
  - asset_type_id: 68
  - asset_source_id: `Titles`
  - business_entity_id: `NA`
  - migrated_rms_id: 3
  - asset_name: from `Title Name`
  - asset_group_id: 1
  - episode_number: 0
  - release_year: null (int)
  - created_on, last_modified_on, imported_on: current UTC timestamp formatted as `YYYY-MM-DD HH:MM:SS.SSS`
  - is_active: true
- Optional debug fields used in processing (not required by DB schema): `title_id`, `series_name`, `title_type`
- Truncate all string columns to at most 10000 characters (cut to 9999 if exceeded)

## Running

Ensure Spark is installed and the PostgreSQL JDBC driver is available.

- Preview mode (no DB write):
```
PREVIEW_ONLY=true ASSET_CSV_PATH=/absolute/path/to/SampleDump.csv \
spark-submit etl_backend_api/etl_jobs/Scarlett_Asset_Load.py
```

- Write to PostgreSQL (append):
```
ASSET_CSV_PATH=/absolute/path/to/SampleDump.csv \
PG_JDBC_URL="jdbc:postgresql://localhost:5432/rightsdb" PG_USER=myuser PG_PASSWORD=mypass \
PG_SCHEMA=public PG_TABLE=Assets \
spark-submit --packages org.postgresql:postgresql:42.7.4 etl_backend_api/etl_jobs/Scarlett_Asset_Load.py
```

## Logs and Output

- The script logs input row counts, distinct Title IDs, final row count, and prints the schema.
- On preview, it displays the top 20 rows without writing.
- On write, it uses JDBC append mode with `batchsize=5000` and `isolationLevel=READ_COMMITTED`.

## Notes

- Ensure the destination table exists and column compatibility is maintained.
- Timestamps are generated in UTC with millisecond precision.
