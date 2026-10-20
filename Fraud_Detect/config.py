import os 

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

T_RAW = "txn.raw"
T_VALID = "txn.validate"
T_ALERTS = "fraud.alerts"
T_STATE = "fraud.state"
T_DLQ = "txn.dlq"

PARTITIONS = 6

def base_conf(**extra):
    conf = {"bootstrap.servers":BOOTSTRAP}
    conf.update(extra)
    return conf