import os
from .base import SourceReader


class JdbcReader(SourceReader):
    def read(self, watermark=None):
        url = f"jdbc:postgresql://localhost:5432/{os.environ['POSTGRES_DB']}"
        table = self.source["table"]
        load_type = self.source.get("load_type", "full")
        wm_col = self.source.get("watermark_column")

        # Incremental + we have a watermark -> only pull rows newer than it.
        # (Postgres evaluates this WHERE using the index on the watermark column.)
        if load_type == "incremental" and wm_col and watermark is not None:
            dbtable = f"(SELECT * FROM {table} WHERE {wm_col} > '{watermark}') AS sub"
        else:
            dbtable = table

        return (
            self.spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", dbtable)
            .option("user", os.environ["POSTGRES_USER"])
            .option("password", os.environ["POSTGRES_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )
