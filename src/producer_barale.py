# Tutorial: Aiven Kafka Quickstart using Python
# Author: Daniel Barale
import json
from kafka import KafkaProducer
import yfinance as yf
from datetime import datetime
import uuid
from faker import Faker
from faker.generator import random
from faker.providers import BaseProvider

class SymbolProvider(BaseProvider):
    def sym_name(self):
        valid_sym_names = [
            'A','B','C','D','E','F','G','H','J','K','L','M','O','P','R','S','T','U','V','W','X','Y','Z'
        ]
        return random.choice(valid_sym_names)

def producer_barale(service_uri, ca_path, cert_path, key_path):
    producer = KafkaProducer(
        bootstrap_servers=service_uri,
        security_protocol="SSL",
        ssl_cafile=ca_path,
        ssl_certfile=cert_path,
        ssl_keyfile=key_path,
        value_serializer=lambda v: json.dumps(v).encode('ascii'),
        key_serializer=lambda v: json.dumps(v).encode('ascii')
    )
    fake = Faker()
    fake.add_provider(SymbolProvider)

    for i in range(0, 12):
        message = "message number {}".format(i+1)
        print("Sending: {}".format(message))
        random_sym = fake.unique.sym_name()
        sym = yf.Ticker(random_sym)
        sym.info.update({'ts': datetime.now().isoformat()})
        print(sym.info)
        producer.send(
            "yfinance_topic",
            key={"id":str(uuid.uuid4())},
            value=sym.info
        )
    producer.flush()
