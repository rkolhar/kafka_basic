#from mysql.connector import connect, Error
import yaml
import sqlalchemy
import json
import requests
from sqlalchemy.orm import *
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

with open('config.yaml', 'r') as cfg:
    config = yaml.safe_load(cfg)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    

last_read_timestamp = config['last_read_timestamp']
if last_read_timestamp is None:
    last_read_timestamp = '2023-01-01 11:45:00'


# create schema registry client 

schema_registry_client = SchemaRegistryClient({   
        'url': config['schema_registry_url'], 
        'basic.auth.user.info': f"{config['auth_user']}:{config['auth_pass']}"
    })

# avro schema
value_subject_name = f"{config['kafka_topic']}-value"
value_schema_str = schema_registry_client.get_latest_version(value_subject_name).schema.schema_str

# avro key schema
key_subject_name = f"{config['kafka_topic']}-key"
key_schema_str = schema_registry_client.get_latest_version(key_subject_name).schema.schema_str

# key and value serializer
key_serializer = AvroSerializer(schema_registry_client, key_schema_str)
value_serializer = AvroSerializer(schema_registry_client, value_schema_str)

# configure producer 

producer = SerializingProducer(
    {
        'bootstrap.servers':  config['kafka_bootstrap_server'],
        'security.protocol': config['security_protocol'],
        'sasl.mechanisms': config['sasl_mechanisms'],
        'sasl.username': config['sasl_username'],
        'sasl.password': config['sasl_password'],
        'key.serializer': key_serializer,
        'value.serializer': value_serializer 
    }
)

try:

    #url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    engine = sqlalchemy.create_engine("postgresql://postgres:postgres@localhost:5432/postgres") 

    print('connecting to engine')
    conn = engine.connect()

    print('connection succeeded')
except ConnectionError as e:
    print(e)

output = conn.execute(f"select * from product ")
rows = output.fetchall()

#print(output)
for row in rows:
    key = output._metadata.keys
    val = dict(zip(key, row))
    print(val)
    producer.produce(topic='product_updates', key=str(val['id']), value=val, on_delivery=delivery_report)
    producer.flush()

# conn.close()
print("Data successfully published to Kafka")