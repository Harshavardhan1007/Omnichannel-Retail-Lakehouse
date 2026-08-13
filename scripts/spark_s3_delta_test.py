import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

load_dotenv()
BUCKET = os.environ["S3_LAKE_BUCKET"]
REGION = os.environ["AWS_REGION"]
PATH = f"s3a://{BUCKET}/delta-test/"

builder = (
    SparkSession.builder.appName("delta-s3-gate")
    # --- Delta Lake: without these two, Delta silently acts like plain Parquet ---
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # --- S3A: how Spark talks to S3 ---
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
)

# jars Spark must download: S3A connector + AWS SDK (Delta jars added by the helper)
extra_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]
spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Writing version 0 (3 rows)...")
spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"]) \
     .write.format("delta").mode("overwrite").save(PATH)

print("Reading it back:")
spark.read.format("delta").load(PATH).show()

print("Writing version 1 (append 1 row)...")
spark.createDataFrame([(4, "d")], ["id", "val"]) \
     .write.format("delta").mode("append").save(PATH)

print("History (expect 2 versions):")
DeltaTable.forPath(spark, PATH).history().select("version", "timestamp", "operation").show(truncate=False)

print("Time travel to version 0 (original 3 rows):")
spark.read.format("delta").option("versionAsOf", 0).load(PATH).show()

spark.stop()
print("\n✅ GATE PASSED: Delta on S3 with version history + time travel.")