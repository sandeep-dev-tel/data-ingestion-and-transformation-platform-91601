import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, length
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

csv_path = "s3://wbd-msc-mstack-rights-dev-aggregation-ue1/SourceFile_PI/PI_Asset_Data_2025-06-10.csv"

# Define the schema to avoid type inference issues
csv_schema = StructType([
    StructField("ASSET_UUID", StringType(), True),
    StructField("ASSET_TYPE", StringType(), True),
    StructField("SERIES_TITLE", StringType(), True),
    StructField("SEASON_TITLE", StringType(), True),
    StructField("SEASON_NUMBER", StringType(), True),
    StructField("PROPERTY_ID", StringType(), True),
    StructField("EPISODE_NUMBER", StringType(), True),
    StructField("PROGRAM_TITLE", StringType(), True),
    StructField("CONTROLLING_NETWORK", StringType(), True),
    StructField("GLOBAL_RELEASE_YEAR", StringType(), True),
    StructField("REPACK_TYPE", StringType(), True),
    StructField("RL_ASSET_ID", StringType(), True),
    StructField("TITLES_TITLE_ID", StringType(), True),
    StructField("FABRIC_ID", StringType(), True),
    StructField("P2_PROGRAM_ID", StringType(), True)
])

# Read the CSV with the predefined schema to treat all columns as strings initially
df = spark.read \
    .option("header", True) \
    .option("mode", "PERMISSIVE") \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .schema(csv_schema) \
    .csv(csv_path)

# Data transformations and type casting
df = df.withColumn("SEASON_NUMBER", col("SEASON_NUMBER").cast(IntegerType()))
df = df.withColumn("REPACK_TYPE", col("REPACK_TYPE").cast(IntegerType()))

csv_columns = [
    "ASSET_UUID",
    "ASSET_TYPE",
    "SERIES_TITLE",
    "SEASON_TITLE",
    "SEASON_NUMBER",
    "PROPERTY_ID",
    "EPISODE_NUMBER",
    "PROGRAM_TITLE",
    "CONTROLLING_NETWORK",
    "GLOBAL_RELEASE_YEAR",
    "REPACK_TYPE",
    "RL_ASSET_ID",
    "TITLES_TITLE_ID",
    "FABRIC_ID",
    "P2_PROGRAM_ID"
]

db_columns = csv_columns

for csv_col, db_col in zip(csv_columns, db_columns):
    df = df.withColumnRenamed(csv_col, db_col)

# Truncate string columns to prevent length errors on insert
MAX_ALLOWED = 10000
TRIM_LENGTH = 9999

for column in df.columns:
    if df.schema[column].dataType.simpleString() == "string":
        df = df.withColumn(
            column,
            when(length(col(column)) > MAX_ALLOWED, col(column).substr(1, TRIM_LENGTH))
            .otherwise(col(column))
        )

# Database connection details
source_url = "jdbc:postgresql://rights-explorer-etl-dev-0.crej7ibabmwr.us-east-1.rds.amazonaws.com:5432/rights_explorer_etl"  # replace
table_name = "rights_canonical_schema.temp_tb_src_pi"

source_props = {
    "user": "rights_explorer_etl_svc",
    "password": "uZTYRdPpyN2Ao20n",
    "driver": "org.postgresql.Driver"
}

# Write the DataFrame to the PostgreSQL table
df.write.jdbc(url=source_url, table=table_name, mode="append", properties=source_props)

job.commit()
