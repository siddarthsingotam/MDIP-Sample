import random
import time

from kafka import KafkaProducer
import json
from datetime import datetime

# Set up the producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Change this to self Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Example message

message = {
    "player_id": 1234,
    "heart_rate_bpm": 110,
    "position_x": 53.2734,
    "position_y": -7.3222,
}

message2 = {
    "type": "object",
    "properties": {
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "sensorId": {
            "type": "string"
        },
        "type": {
            "type": "string",
            "enum": ["heartbeat", "oxygen_saturation", "accelerometer"]
        },
        "data": {
            "type": "object",
            "properties": {
                "heartbeat": {
                    "type": "number"
                },
                "oxygen_saturation": {
                    "type": "number"
                },
                "acceleration": {
                    "type": "object",
                    "properties": {
                        "x": {"type": "number"},
                        "y": {"type": "number"},
                        "z": {"type": "number"}
                    }
                },
                "angularVelocity": {
                    "type": "object",
                    "properties": {
                        "roll": {"type": "number"},
                        "pitch": {"type": "number"}
                    }
                }
            }
        }
    },
    "required": ["timestamp", "sensorId", "type", "data"]
}


# Create heartbeat data
def create_heartbeat_data():
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensorId": "sensor-12324",
        "type": 'heartbeat',
        "data": {
            "heartbeat": random.randint(50, 200) # Simulates heartbeat in a specific range for example
        }
    }


def create_accelerometer_data():
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensorId": "sensor-789",
        "type": "accelerometer",
        "data": {
            "acceleration": {
                "x": random.uniform(-1.0, 1.0),
                "y": random.uniform(-1.0, 1.0),
                "z": random.uniform(-10, 10)
            },
            "angularVelocity": {
                "roll": random.uniform(-0.1, 0.1),
                "pitch": random.uniform(-0.1, 0.1)
            }
        }
    }


# Send Data
def send_data_to_kafka():
    topic = "python"  # I put my topic that I created with Kafka here
    try:
        while True:
            heart_beat_data = create_heartbeat_data()
            accelerometer_data = create_accelerometer_data()

            # send to producer
            producer.send(topic, heart_beat_data)
            producer.send(topic, accelerometer_data)

            # local print
            print(f"Sent data to Kafka server: {heart_beat_data}")
            print(f"Sent data to Kafka server: {accelerometer_data}")

            # time gaps
            time.sleep(3)  # sleep for 3 seconds

    except KeyboardInterrupt:
        # Close Producer
        producer.close()
        print("Ctrl + C pressed, Exiting loop....")


if __name__ == "__main__":
    send_data_to_kafka()

# producer.send('python', message)
#
# # Close the producer connection
# producer.close()


