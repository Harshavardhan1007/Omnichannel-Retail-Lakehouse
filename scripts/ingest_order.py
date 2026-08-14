import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

load_dotenv()
BUCKET = os.environ["S3_LAKE_BUCKET"]
REGION = os.environ["AWS_REGION"]

builder = (
    SparkSession.builder.appName("ingest-orders")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
)

extra_packages = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.3",   # NEW: JDBC driver so Spark can read Postgres
]
spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# from the host, Postgres is at localhost:5432 (the container's mapped port)
jdbc_url = f"jdbc:postgresql://localhost:5432/{os.environ['POSTGRES_DB']}"

print("Reading orders from Postgres...")
orders = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "orders")
    .option("user", os.environ["POSTGRES_USER"])
    .option("password", os.environ["POSTGRES_PASSWORD"])
    .option("driver", "org.postgresql.Driver")
    .load()
)
print("Rows read from Postgres:", orders.count())
orders.show(5)

target = f"s3a://{BUCKET}/landing/oltp/orders/"
print(f"Writing to {target} as Delta...")
orders.write.format("delta").mode("overwrite").save(target)

print("Reading back from the lake:")
print("Rows in lake:", spark.read.format("delta").load(target).count())

spark.stop()
print("\n✅ Orders ingested from Postgres → S3 Delta lake.")