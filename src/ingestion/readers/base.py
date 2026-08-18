from abc import ABC, abstractmethod

class SourceReader(ABC):
    def __init__(self, spark, source_cfg):
        self.spark = spark
        self.source = source_cfg      # one entry from sources.yml

    @abstractmethod
    def read(self):
        """Return a Spark DataFrame of the source data."""
        ...