from confluent_kafka.admin import NewTopic , AdminClient
from Fraud_Detect import config as C



WEEK_MS = str(7*24*3600*1000)

TOPICS = [
    NewTopic(C.T_RAW, C.PARTITIONS , 1 , config={"retention.ms":WEEK_MS}),
    NewTopic(C.T_VALID,C.PARTITIONS, 1 , config ={"retention.ms":WEEK_MS}),
    NewTopic(C.T_ALERTS, 3,1, config= {"retention.ms":WEEK_MS} ),
    NewTopic(C.T_STATE, C.PARTITIONS , 1 ,  config={
        "cleanup.policy":"compact",
        "min.cleanable.dirty.ratio":"0.1",
        "segment.ms":"60000",
    }),
    NewTopic(C.T_DLQ, 1 ,1,config={"retention.ms":WEEK_MS})
]   

def main():
    admin = AdminClient(C.base_conf())
    for topic , future in admin.create_topics(TOPICS).items():
        try:
            future.result()
            print({topic})
        except Exception as exc:
            print(f"exc,{topic} {exc}")


if __name__ == "__main__" :
    main()