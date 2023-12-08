from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import requests
import json

response = requests.get('https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json')
data_velib = response.json()

information_data = []
for i in data_velib['data']['stations']:
    information_data.append(i)

def build_producer():

    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        for i in information_data:
            producer.send('mon_topic', i)
    except KafkaError as e:
        print(f"Erreur Kafka: {e}")

build_producer()

