from confluent_kafka import Producer
from airflow.hooks.base import BaseHook

class KafkaProducerHook:
    def __init__(self, connection_id=None, group_id=None):
        if not connection_id or not group_id:
            raise ValueError("connection_id and group_id are required")
        
        connection = BaseHook.get_connection(self.connection_id)
        print(connection)
        # self.conf = {
        #     'bootstrap.servers': connection_id,
        #     'group.id': group_id,
        #     'default.topic.config': {'acks': 'all'}
        # }
        # self.producer = Producer(self.conf)

    @classmethod
    def from_conf(cls, conf):
        if not isinstance(conf, dict):
            raise ValueError("conf must be a dictionary")
        instance = cls.__new__(cls)
        instance.conf = conf
        instance.producer = Producer(instance.conf)
        return instance

    # def produce(self, topic, value, key=None):
    #     self.producer.produce(topic, value, key=key)
    #     self.producer.flush()