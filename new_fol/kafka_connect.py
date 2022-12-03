from confluent_kafka.admin import AdminClient,NewTopic, NewPartitions
from confluent_kafka.error import KafkaException,KafkaError
from confluent_kafka.serializing_producer import SerializingProducer
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka import Consumer,Producer
from time import sleep

class KafkaClient:
    def __init__(self,config,replication_factor=1):
        """
        Initial entrypoint into using configured Kafka broker.
        """
        self.config = config
        self.replication_factor = replication_factor
    
    def create_producer(self) -> bool:
        try:
            self.producer = Producer(self.config)
            return True
        except Exception as err:
            raise Exception(err)
    def close_producer(self):
        self.producer = None
    def create_topic(self,topic,partitions=5):
        """
        Creates a new topic on Kafka cluster if it does not currently exist
        """
        admin_client = AdminClient(self.config)
        if self.__check_topic_exists(admin_client,topic):
            return False
        topic_config = {
            "delete.retention.ms":10800000,
            "retention.ms":10800000
        }
        topic_list = []
        topic_list.append(NewTopic(topic=topic,num_partitions=partitions,config=topic_config,replication_factor=self.replication_factor))
        try:
            fs=admin_client.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    print("Topic {} created".format(topic))
                except Exception as e:
                    print("Failed to create topic {}: {}".format(topic, e))
            print("Waited to create topics")
            return True
        except Exception as err:
            print(err)
            raise
    
    def __check_topic_exists(self,admin_client,topic):
        topic_metadata = admin_client.list_topics().topics
        
        topics = [x for x in topic_metadata]
        found = False
        for t in topics:
            if t == topic:
                found = True
                break
            
        return found
    
    def message_callback(self):
        pass
    
    def produce_message(self,topic,key,value,partition=0):
        """
        For the provided topic name will publish message to Kafka broker. Exceptions might be raised if settings don't match below. The message key
        needs to be serializable
        """
        try:
            self.producer.produce(topic=topic,key=key,value=value,partition=partition)
            self.producer.poll(0)
        except Exception  as krr:
            print(krr)
            raise Exception('Kafka Error raised when publishing Kafka message {}'.format(krr))
        
        
    