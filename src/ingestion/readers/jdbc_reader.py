import os
from .base import SourceReader

class JdbcReader(SourceReader):
    def read(self):
        url = f"jdbc:postgresql://localhost:5432/{os.environ['POSTGRES_DB']}"
        return (
            self.spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", self.source["table"])
            .option("user", os.environ["POSTGRES_USER"])
            .option("password", os.environ["POSTGRES_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )