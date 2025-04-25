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


# Create heartbeat data
def create_heartbeat_data():
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensorId": "sensor-12324",
        "type": 'heartbeat',
        "data": {
            "heartbeat": random.randint(50, 200)  # Simulates heartbeat in a specific range for example
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


def hr_data():
    return {
        "HR_data": {
            "rrData": [384],
            "Movesense_series": "174630000192",
            "Timestamp_UTC": 1743460235,
            "average_bpm": 108.7813
        }
    }


def ecg_data():
    return {
        "ECG_data": {
            "Samples": [-62342, -51680, -43311, -35942, -29149, -23343, -18437, -14248, -10712, -7628, -4838, -2323,
                        -90, 1953, 3926, 5939],
            "Movesense_series": "174630000192",
            "Timestamp_UTC": 1743460235,
            "Timestamp_ms": 29889
        }
    }


def imu9_data():
    return {
        "IMU9_data": {
            "Movesense_series": "174630000192",
            "Timestamp_UTC": 1743460235,
            "Timestamp_ms": 30348,
            "ArrayAcc": [
                {"x": 8.198, "y": -5.47, "z": 1.235},
                {"x": 8.279, "y": -5.37, "z": 1.278},
                {"x": 8.126, "y": -5.296, "z": 1.251},
                {"x": 7.935, "y": -5.202, "z": 1.232}
            ],
            "ArrayGyro": [
                {"x": 2.8, "y": 6.3, "z": 5.6},
                {"x": 0.56, "y": 9.52, "z": 8.82},
                {"x": -1.75, "y": 7.0, "z": 11.2},
                {"x": 1.68, "y": -0.63, "z": 11.06}
            ],
            "ArrayMagn": [
                {"x": 42.6, "y": -28.5, "z": 3.45},
                {"x": 44.1, "y": -27.45, "z": 5.7},
                {"x": 43.35, "y": -28.2, "z": 4.05},
                {"x": 43.2, "y": -25.05, "z": 3.3}
            ]
        }
    }


def gnss_data():
    return {
        "GNSS_data": {
            "GNSS_sensor_ID": "self.device_id",
            "Date": "date",
            "Latitude": "lat",
            "Longitude": "lon"
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
