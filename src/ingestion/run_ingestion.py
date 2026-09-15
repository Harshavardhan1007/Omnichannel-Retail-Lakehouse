import argparse
import os
import yaml
from dotenv import load_dotenv
from .spark_session import get_spark
from .readers.factory import make_reader

def find_source(source_id):
    with open("config/sources.yml") as f:
        sources = yaml.safe_load(f)["sources"]
    for s in sources:
        if s["source_id"] == source_id:
            return s
    raise ValueError(f"source_id '{source_id}' not found in sources.yml")

def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    args = parser.parse_args()

    cfg = find_source(args.source)
    spark = get_spark(app_name=f"ingest-{args.source}")

    reader = make_reader(spark, cfg)
    print(f"Reading {args.source}...")
    df = reader.read()
    count = df.count()

    bucket = os.environ["S3_LAKE_BUCKET"]
    target = f"s3a://{bucket}/landing/{cfg['system']}/{cfg['entity']}/"
    print(f"Writing {count} rows to {target}")
    df.write.format("delta").mode("overwrite").save(target)

    spark.stop()
    print(f"✅ Ingested {args.source}: {count} rows")

if __name__ == "__main__":
    main()