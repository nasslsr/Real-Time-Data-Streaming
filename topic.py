from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

def create_topic():
    try : 
        admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='test')
        topic_list = [NewTopic(name="mon_topic_test", num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except KafkaError as e:
        print(f"Erreur Kafka: {e}")

create_topic()