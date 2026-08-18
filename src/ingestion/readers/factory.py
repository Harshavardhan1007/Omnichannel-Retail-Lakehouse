from .jdbc_reader import JdbcReader

READERS = {"jdbc": JdbcReader}     # protocol string -> reader class

def make_reader(spark, source_cfg):
    proto = source_cfg["protocol"]
    if proto not in READERS:
        raise ValueError(f"No reader for protocol '{proto}'")
    return READERS[proto](spark, source_cfg)