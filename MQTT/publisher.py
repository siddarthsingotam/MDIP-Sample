# mqtt_publisher.py
import json
import time
import random
from datetime import datetime
import paho.mqtt.client as mqtt


PICO_ID = "qwert_1234"

def hr_data():
    return {
        "HR_data": {
            "rrData": [384],
            "Pico_ID": PICO_ID,
            "Movesense_series": "174630000192",
            "Timestamp_UTC": int(time.time()),
            "average_bpm": 108.7813
        }
    }


def ecg_data():
    return {
        "ECG_data": {
            "Pico_ID": PICO_ID,
            "Samples": [-62342, -51680, -43311, -35942, -29149, -23343, -18437, -14248, -10712, -7628, -4838, -2323,
                        -90, 1953, 3926, 5939],
            "Movesense_series": "174630000192",
            "Timestamp_UTC": int(time.time()),
            "Timestamp_ms": 29889
        }
    }


def imu9_data():
    return {
        "IMU9_data": {
            "Pico_ID": PICO_ID,
            "Movesense_series": "174630000192",
            "Timestamp_UTC": int(time.time()),
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
            "Pico_ID": PICO_ID,
            "GNSS_sensor_ID": "device123",
            "Date": datetime.now().isoformat(),
            "Latitude": 37.7749 + random.uniform(-0.01, 0.01),
            "Longitude": -122.4194 + random.uniform(-0.01, 0.01)
        }
    }

def all_data():
    return {
        "HR_data": hr_data()["HR_data"],
        "ECG_data": ecg_data()["ECG_data"],
        "IMU9_data": imu9_data()["IMU9_data"],
        "GNSS_data": gnss_data()["GNSS_data"]
    }


class SensorPublisher:
    def __init__(self):
        # MQTT Configuration
        self.mqtt_broker = "d4e877f7c282469c87fe4307599ad40c.s1.eu.hivemq.cloud"  # e.g., broker.hivemq.cloud
        self.mqtt_port = 8883  # TLS port
        self.mqtt_username = "AtomBerg1" # SAMPLE USERNAME
        self.mqtt_password = "AtomBerg1"
        self.mqtt_client_id = f"sensor-publisher-XXXX"

        # MQTT Topics
        self.topic_hr = "sensors/hr"
        self.topic_ecg = "sensors/ecg"
        self.topic_imu = "sensors/imu"
        self.topic_gnss = "sensors/gnss"

        # Initialize MQTT client
        self.mqtt_client = mqtt.Client(client_id=self.mqtt_client_id, protocol=mqtt.MQTTv5)
        self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)
        self.mqtt_client.tls_set()  # Enable TLS by default for HiveMQ Cloud

        # Connect MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected to MQTT broker!")
        else:
            print(f"Failed to connect to MQTT broker with code {rc}")

    def get_time_ms(self):
        return time.time_ns() // 1_000_000

    def publish_handler(self, method, topic):
        """A handler (child method) that takes a method and a topic and publishes it along with a timestamp of
         publishing in milliseconds"""
        # A variable for publish time ms
        publish_time_ms = "publish_time_ms"
        x = method
        x[publish_time_ms] = self.get_time_ms()
        self.mqtt_client.publish(topic, json.dumps(x))
        print(f"Published {topic} at {x[publish_time_ms]}")

    def publish_data(self):
        """Publish sensor data to MQTT broker"""
        try:
            # Publish handler
            self.publish_handler(hr_data(), self.topic_hr)
            self.publish_handler(ecg_data(), self.topic_ecg)
            self.publish_handler(imu9_data(), self.topic_imu)
            self.publish_handler(gnss_data(), self.topic_gnss)

        except Exception as e:
            print(f"Error publishing data: {e}")

    def start(self):
        """Start the sensor publisher"""
        try:
            # Connect to MQTT broker
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)

            # Start MQTT loop in the background
            self.mqtt_client.loop_start()

            print("Sensor publisher started!")

            # Publish data periodically
            while True:
                print(100 * "-")
                self.publish_data()
                time.sleep(5)  # Publish every 5 seconds

        except KeyboardInterrupt:
            print("Publisher stopped by user")
        except Exception as e:
            print(f"Error in publisher: {e}")
        finally:
            # Clean up
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            print("Publisher shutdown complete")


if __name__ == "__main__":
    publisher = SensorPublisher()
    publisher.start()
