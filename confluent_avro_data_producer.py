
import datetime
import json
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer #for value serializiation
from confluent_kafka.serialization import StringSerializer  #for key serializiation
import pandas as pd
import mysql.connector
from mysql.connector import Error


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("=====================")

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': '',
    'sasl.mechanisms': '',
    'security.protocol': '',
    'sasl.username': '',
    'sasl.password': ''
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': '',
  'basic.auth.user.info': '{}:{}'.format('', '')
})



# Database connection
connection = mysql.connector.connect(
    host='127.0.0.1',
    user='',
    password='',
    database='kafka_products'
)
cursor = connection.cursor()

# Fetch the latest Avro schema for the value
subject_name = 'product-data-dev-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

# Load the last read timestamp from the config file
config_data = {}

try:
    with open('config.json') as f:
        config_data = json.load(f)
        last_read_timestamp = config_data.get('last_read_timestamp')
except FileNotFoundError:
    last_read_timestamp = '1900-01-01 00:00:00'

# Set a default value for last_read_timestamp
if last_read_timestamp is None:
    last_read_timestamp = '1900-01-01 00:00:00'

# Use the last_read_timestamp in the SQL query
query = "SELECT * FROM product WHERE last_updated > '{}'".format(last_read_timestamp)

# Execute the SQL query
cursor.execute(query)

# Check if there are any rows fetched
rows = cursor.fetchall()
if not rows:
    print("No rows to fetch.")
else:
    # Iterate over the cursor and produce to Kafka
    for row in rows:
        # Get the column names from the cursor description
        columns = [column[0] for column in cursor.description]

        # Create a dictionary from the row values
        value = dict(zip(columns, row))

        # Produce to Kafka
        print("Available keys:", value.keys())  # Debugging step
        producer.produce(topic='product-data-dev', key=str(value['id']), value=value, on_delivery=delivery_report)
        producer.flush()

# Fetch any remaining rows to consume the result
cursor.fetchall()

# getting latest uodated timestamp for record 
query = "SELECT MAX(last_updated) FROM product"
cursor.execute(query)

# Fetch the result
result = cursor.fetchone()
max_date = result[0]  # Assuming the result is a single value

# Convert datetime object to string representation
max_date_str = max_date.strftime("%Y-%m-%d %H:%M:%S")

# Update the value in the config.json file
config_data['last_read_timestamp'] = max_date_str

with open('config.json', 'w') as file:
    json.dump(config_data, file)

# Close the cursor and database connection
cursor.close()
connection.close()

print("Data successfully published to Kafka")