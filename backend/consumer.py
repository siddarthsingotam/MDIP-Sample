from kafka import KafkaConsumer
import json

# Set up the consumer
consumer = KafkaConsumer(
    'python',  # Topic name
    bootstrap_servers='localhost:9092',  # Kafka broker address
    group_id='test-group',  # Consumer group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for messages...")

# Consume messages from the "test" topic
for message in consumer:
    print(f"Received: {message.value}")
