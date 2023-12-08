from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic


def build_consumer():
    consumer = KafkaConsumer('mon_topic', bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
    try:
        for message in consumer:
            print(message.value)
    except KafkaError as e:
        print(f"Erreur Kafka: {e}")

build_consumer()

