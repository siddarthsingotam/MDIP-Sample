import json
import os
import time

from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point
from kafka import KafkaConsumer

# Setting up environment
dotenv_path = os.path.join("../keys/keys.env")
load_dotenv(dotenv_path=dotenv_path)

# Set up the consumer
consumer = KafkaConsumer(
    'python',  # Topic name
    bootstrap_servers='localhost:9092',  # Kafka broker address
    group_id='test-group',  # Consumer group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Set up the InfluxDB client
token = os.getenv("INFLUXDB_TOKEN_1")
org = "MDIP-Sample"
host = "https://eu-central-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org)

database = "Consumer_Data"

print("Waiting for messages...")

# Consume messages from the "python" topic and write to InfluxDB
for message in consumer:
    data = message.value
    print(f"Received: {data}")

    # Create a point and write to InfluxDB
    point = (
        Point("kafka_data")
        .tag("type", data["type"])
        .field("sensorId", data["sensorId"])
        .time(message.timestamp)
    )

    client.write(database=database, record=point)
    time.sleep(1)  # separate points by 1 second

print("Complete. Return to the InfluxDB UI.")
