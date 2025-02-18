
import json
import os
import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from datetime import datetime


# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': '',
    'sasl.mechanisms': '',
    'security.protocol': '',
    'sasl.username': '',
    'sasl.password': '',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest' 
}


# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': '',
  'basic.auth.user.info': '{}:{}'.format('', '')
})




# Fetch the latest Avro schema for the value
subject_name = 'product-data-dev-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})


# # To handle serialization of datetime objects,defining a custom encoder.
def datetime_encoder(obj):
    if isinstance(obj, datetime):  # Check if it's a datetime instance
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# Path to the separate JSON file for each consumer
file_path = 'consumer2.json'  

# Python function to load append the json string data into json file.
def write_to_json_file(json_string, file_path):
    with open(file_path, 'a') as file:
        file.write(json_string + '\n')

# Subscribe to the 'retail_data' topic
consumer.subscribe(['product-data-dev'])
# Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        
        #Change the category column to lowercase,in source it's in uppercase.
        msg.value()['category'] = msg.value()['category'].lower() 
        
        # updating the price to half if product belongs to 'category a'
        if msg.value()['category'] == 'category a':
            
            msg.value()['price'] = msg.value()['price'] * 0.5
            msg.value()['price'] = round(msg.value()['price'],2)
            
        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))
        json_string = json.dumps(msg.value(), default=datetime_encoder)

        def write_to_json_file(json_string, file_path):
            with open(file_path, 'a') as file:
                file.write(json_string + '\n')

        # Check if the file exists
        if not os.path.isfile(file_path):
            # Create the file and write the initial data
            with open(file_path, 'w') as file:
                file.write(json_string + '\n')
        else:
            # Append the data to the existing file
            write_to_json_file(json_string, file_path)
            print("json_string data is added to the JSON file.")
        file.close()
           
except KeyboardInterrupt:
    pass
finally:
    consumer.close()