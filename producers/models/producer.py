"""Producer base-class providing common utilites and functionality"""
#Pragnesh saved - 10/24/2021-- start of project
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
        client=None,
        topic=None
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            'broker.url': "PLAINTEXT://localhost:9092",
            'schema.registry.url': "http://localhost:8081",
            # TODO
        }
        
        #PGXXX  logger.info(f"broker url {self.broker_properties['schema.registry.url']}")
         
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient({"url": self.broker_properties['schema.registry.url']})
        self.producer = AvroProducer({"bootstrap.servers": self.broker_properties['broker.url']}, schema_registry=schema_registry)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        self.client = AdminClient({"bootstrap.servers": self.broker_properties['broker.url']})
        self.topic = NewTopic(topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)
        futures = self.client.create_topics(
            [self.topic]
        )
        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                print(f"topic {self.topic_name} not created with error {e}")

        #logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        #client = AdminClient({"bootstrap.servers": self.broker_properties['broker.url']})
        #if self.topic_name  in Producer.existing_topics:
        #    self.client.delete_topics([self.topic])
        #    Producer.existing_topics.delete(self.topic_name)
        self.producer.flush()
        #logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
