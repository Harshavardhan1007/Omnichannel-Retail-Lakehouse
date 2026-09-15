import argparse
import os
import uuid
import yaml
from dotenv import load_dotenv
from pyspark.sql import functions as F
from .spark_session import get_spark
from .readers.factory import make_reader
from . import ops


def load_sources():
    with open("config/sources.yml") as f:
        return yaml.safe_load(f)["sources"]


def find_source(sources, source_id):
    for s in sources:
        if s["source_id"] == source_id:
            return s
    raise ValueError(f"source_id '{source_id}' not found in sources.yml")


def ingest_one(spark, cfg, bucket):
    source_id = cfg["source_id"]
    run_id = str(uuid.uuid4())
    load_type = cfg.get("load_type", "full")
    wm_col = cfg.get("watermark_column")

    # For incremental sources, find where we left off last time.
    watermark = ops.get_last_watermark(source_id) if load_type == "incremental" else None
    print(f"Reading {source_id} (load_type={load_type}, since={watermark}) ...")

    reader = make_reader(spark, cfg)
    df = reader.read(watermark).cache()      # cache so count + max don't re-query
    count = df.count()

    # New watermark = the newest updated_at in what we just read (keep old if nothing new).
    new_wm = watermark
    if count > 0 and wm_col:
        new_wm = df.agg(F.max(wm_col)).collect()[0][0]

    # First run (no watermark) writes a fresh snapshot; later runs append the deltas.
    mode = "append" if (load_type == "incremental" and watermark is not None) else "overwrite"
    target = f"s3a://{bucket}/landing/{cfg['system']}/{cfg['entity']}/"

    if count > 0:
        df.write.format("delta").mode(mode).save(target)
    df.unpersist()

    ops.record_run(source_id, run_id, "SUCCESS", load_type, count, new_wm)
    print(f"  ✅ {source_id}: {count:,} rows ({mode}); watermark now {new_wm}")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, help="a source_id, or 'all'")
    args = parser.parse_args()

    ops.ensure_table()                       # create ops.ingest_runs if missing
    sources = load_sources()
    bucket = os.environ["S3_LAKE_BUCKET"]
    to_run = sources if args.source == "all" else [find_source(sources, args.source)]

    spark = get_spark(app_name=f"ingest-{args.source}")
    for cfg in to_run:
        ingest_one(spark, cfg, bucket)
    spark.stop()
    print(f"\n✅ Done. Ingested {len(to_run)} source(s).")


if __name__ == "__main__":
    main()
