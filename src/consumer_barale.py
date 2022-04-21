# Tutorial: Aiven Kafka Quickstart using Python
# Author: Daniel Barale
import json
from kafka import KafkaConsumer

def consumer_barale(service_uri, ca_path, cert_path, key_path):
    consumer = KafkaConsumer(
        bootstrap_servers=service_uri,
        auto_offset_reset='earliest',
        security_protocol="SSL",
        ssl_cafile=ca_path,
        ssl_certfile=cert_path,
        ssl_keyfile=key_path,
        consumer_timeout_ms=1000,
        value_deserializer=lambda v: json.loads(v.decode('ascii')),
        key_deserializer=lambda v: json.loads(v.decode('ascii'))
    )

    consumer.subscribe(["yfinance_topic"])
    for message in consumer:
        print(message.key)
        print(message.value)
    consumer.close()
    
