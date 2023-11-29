import yaml
from datetime import datetime
import sqlalchemy
import json
from sqlalchemy.orm import *
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from concurrent.futures import ThreadPoolExecutor
import threading

with open('config.yaml', 'r') as cfg:
    config = yaml.safe_load(cfg)


schema_registry_client = SchemaRegistryClient({   
        'url': config['schema_registry_url'], 
        'basic.auth.user.info': f"{config['auth_user']}:{config['auth_pass']}"
    })

# avro value schema
value_subject_name = f"{config['kafka_topic']}-value"
value_schema_str = schema_registry_client.get_latest_version(value_subject_name).schema.schema_str

# avro key schema
key_subject_name = f"{config['kafka_topic']}-key"
key_schema_str = schema_registry_client.get_latest_version(key_subject_name).schema.schema_str

# key and value deserializer
key_deserializer = AvroDeserializer(schema_registry_client, key_schema_str)
value_deserializer = AvroDeserializer(schema_registry_client, value_schema_str)

def datetime_encoder(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()


def create_consumer():

    
    consumer = DeserializingConsumer(
        {
            'bootstrap.servers': config['kafka_bootstrap_server'],
            'security.protocol': config['security_protocol'],
            'sasl.mechanisms': config['sasl_mechanisms'],
            'sasl.username': config['sasl_username'],
            'sasl.password': config['sasl_password'],
            'key.deserializer': key_deserializer,
            'value.deserializer': value_deserializer,
            'group.id': config['group_id'],
            'auto.offset.reset': config['auto_offset_reset']
        }
    )

    
    # Subscribe to the 'product_updates' topic
    consumer.subscribe(['product_updates'])
    return consumer

def consume_msg(consumer, consumer_id):
    
    file_path = f"consumer_{consumer_id}.json"

    # Continually read messages from Kafka
    try:
        while True:

            msg = consumer.poll(1000)

            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue
            
            json_string = json.dumps(msg.value(), default=datetime_encoder)
            with open(file_path, 'a') as file:
                file.write(json_string + '\n')
                print(f"json string is added to the {file_path} file.")
            #file.close()

    except Exception as e:
        print(f"Exception in consumer {consumer_id}: {e}")
    finally:
        print(f'Consumed messages from consumer_{consumer_id}')
    consumer.close()

# Number of consumers
num_consumers = 5

consumers = [create_consumer() for _ in range(num_consumers)]

threads = []
for i, consumer in enumerate(consumers):

    consume_thread = threading.Thread(target=consume_msg, args=(consumer, i))
    consume_thread.start()
    threads.append(consume_thread)

# Keep the main thread alive
for thread in threads:
    thread.join()

print("All threads have finished.")

