import argparse
import os
import yaml
from dotenv import load_dotenv
from .spark_session import get_spark
from .readers.factory import make_reader


def load_sources():
    with open("config/sources.yml") as f:
        return yaml.safe_load(f)["sources"]


def find_source(sources, source_id):
    for s in sources:
        if s["source_id"] == source_id:
            return s
    raise ValueError(f"source_id '{source_id}' not found in sources.yml")


def ingest_one(spark, cfg, bucket):
    reader = make_reader(spark, cfg)
    print(f"Reading {cfg['source_id']} ...")
    df = reader.read()
    count = df.count()
    target = f"s3a://{bucket}/landing/{cfg['system']}/{cfg['entity']}/"
    print(f"  writing {count:,} rows to {target}")
    df.write.format("delta").mode("overwrite").save(target)
    print(f"  ✅ {cfg['source_id']}: {count:,} rows")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="a source_id, or 'all'")
    args = parser.parse_args()

    sources = load_sources()
    bucket = os.environ["S3_LAKE_BUCKET"]

    if args.source == "all":
        to_run = sources
    else:
        to_run = [find_source(sources, args.source)]

    spark = get_spark(app_name=f"ingest-{args.source}")
    for cfg in to_run:
        ingest_one(spark, cfg, bucket)
    spark.stop()
    print(f"\n✅ Done. Ingested {len(to_run)} source(s).")


if __name__ == "__main__":
    main()
