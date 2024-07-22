import json
from confluent_kafka import Consumer, KafkaException

class KafkaConsumerHook:
    def __init__(self, conf, topic):
        self.conf = conf
        self.topic = topic

    def get_consumer(self):
        return Consumer(self.conf)

    def consume_messages(self, max_records=1):
        consumer = self.get_consumer()
        msgs = []
        try:
            consumer.subscribe([self.topic])
            remaining_records = max_records
            while remaining_records > 0:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    print("No message received.")
                elif msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        print(f"End of partition reached {msg.partition()}")
                    else:
                        raise KafkaException(msg.error())
                else:
                    remaining_records -= 1
                    payload = json.loads(msg.value().decode())
                    print(f"Consumed message: {payload}")
                    msgs.append(payload)
        finally:
            consumer.close()
        return msgs

# Example usage:
# conf = {
#     'bootstrap.servers': 'host.docker.internal:9092',
#     'group.id': 'mygroup',
#     'auto.offset.reset': 'latest'
# }
# hook = KafkaConsumerHook(
#     conf=conf,
#     topic='your_topic_name',
# )
# hook.consume_latest_message(max_records=1)
