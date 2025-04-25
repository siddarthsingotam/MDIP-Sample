# mqtt_subscriber.py
import json
import time
import os
from datetime import datetime
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point

# Load environment variables for InfluxDB credentials
KEY_PATH = r"..\keys\keys.env"
load_dotenv(KEY_PATH)

token = os.getenv("ALL_ACCESS_TOKEN")
if not token:
    raise ValueError("Environment variable ALL_ACCESS_TOKEN is not set")

# InfluxDB Configuration
org = "MDIP-Sample"
host = "https://eu-central-1-1.aws.cloud2.influxdata.com"
database = "Ice_Hockey_Metrics"

measurement = "player_metrics"

measurement_hr = "measurement_heart_rate"
measurement_ecg = "measurement_ecg"
measurement_imu = "measurement_imu"
measurement_gnss = "measurement_gnss"


# Initialize InfluxDB client
influx_client = InfluxDBClient3(host=host, token=token, org=org)


class MQTTSubscriber:
    def __init__(self):
        # MQTT Configuration
        self.mqtt_broker = "d4e877f7c282469c87fe4307599ad40c.s1.eu.hivemq.cloud"
        self.mqtt_port = 8883  # TLS port
        self.mqtt_username = "AtomBerg1"  # Update with your username
        self.mqtt_password = "AtomBerg1"  # Update with your password
        self.mqtt_client_id = f"sensor-subscriber-XXXX"

        # MQTT Topics to subscribe to
        self.topics = [
            "sensors/heart_rate",
            "sensors/ecg",
            "sensors/imu",
            "sensors/gnss",
            "sensors/all"  # Special topic containing all sensor data
        ]

        # Initialize MQTT client
        self.mqtt_client = mqtt.Client(client_id=self.mqtt_client_id, protocol=mqtt.MQTTv5)
        self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)
        self.mqtt_client.tls_set()  # Enable TLS for HiveMQ Cloud

        # Connect MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected to MQTT broker!")

            # Subscribe to all sensor topics
            for topic in self.topics:
                client.subscribe(topic, qos=1)  # deliver message least once
                print(f"Subscribed to {topic}")
        else:
            print(f"Failed to connect to MQTT broker with code {rc}")

    def on_message(self, client, userdata, msg):
        print(f"Received message on topic {msg.topic}")
        try:
            # Parse JSON payload
            payload = json.loads(msg.payload.decode())

            # Process the message based on the topic
            if msg.topic == "sensors/all":
                # Handle combined data from all sensors
                # self.process_all_data(payload)
                pass
            elif msg.topic == "sensors/heart_rate":
                # Handle heart rate data
                self.process_hr_data(payload.get("HR_data", {}))
            elif msg.topic == "sensors/ecg":
                # Handle ECG data
                self.process_ecg_data(payload.get("ECG_data", {}))
            elif msg.topic == "sensors/imu":
                # Handle IMU data
                self.process_imu_data(payload.get("IMU9_data", {}))
            elif msg.topic == "sensors/gnss":
                # Handle GNSS data
                self.process_gnss_data(payload.get("GNSS_data", {}))

        except Exception as e:
            print(f"Error processing message: {e}")

    # def process_all_data(self, data):
    #     """Process data from all sensors"""
    #     try:
    #         # Process each data type if it exists in the payload
    #         if "HR_data" in data:
    #             self.process_hr_data(data["HR_data"])
    #
    #         if "ECG_data" in data:
    #             self.process_ecg_data(data["ECG_data"])
    #
    #         if "IMU9_data" in data:
    #             self.process_imu_data(data["IMU9_data"])
    #
    #         if "GNSS_data" in data:
    #             self.process_gnss_data(data["GNSS_data"])
    #
    #     except Exception as e:
    #         print(f"Error processing all data: {e}")

    def process_hr_data(self, hr_data):
        """Process and store heart rate data in InfluxDB"""
        try:
            if not hr_data:
                print("Empty HR data received")
                return

            print(f"Processing HR data: {hr_data}")

            # Serialize rrData as JSON
            serialized_rrData = json.dumps({"data": hr_data.get("rrData", [])})

            # Create point for InfluxDB
            point = (
                Point(measurement_hr)
                .tag("Movesense_series", hr_data.get("Movesense_series", "unknown"))
                .field("rrData", serialized_rrData)
                .field("Timestamp_UTC", hr_data.get("Timestamp_UTC", int(time.time())))
                .field("average_bpm", hr_data.get("average_bpm", 0))
            )

            # Write to InfluxDB
            influx_client.write(database=database, record=point)
            print("HR data stored in InfluxDB")

        except Exception as e:
            print(f"Error processing HR data: {e}")

    def process_ecg_data(self, ecg_data):
        """Process and store ECG data in InfluxDB"""
        try:
            if not ecg_data:
                print("Empty ECG data received")
                return

            print(f"Processing ECG data")

            # Serialize samples as JSON
            serialized_samples = json.dumps({"data": ecg_data.get("Samples", [])})

            # Create point for InfluxDB
            point = (
                Point(measurement_ecg)
                .tag("Movesense_series", ecg_data.get("Movesense_series", "unknown"))
                .field("Samples", serialized_samples)
                .field("Timestamp_UTC", ecg_data.get("Timestamp_UTC", int(time.time())))
                .field("Timestamp_ms", ecg_data.get("Timestamp_ms", 0))
            )

            # Write to InfluxDB
            influx_client.write(database=database, record=point)
            print("ECG data stored in InfluxDB")

        except Exception as e:
            print(f"Error processing ECG data: {e}")

    def process_imu_data(self, imu_data):
        """Process and store IMU data in InfluxDB"""
        try:
            if not imu_data:
                print("Empty IMU data received")
                return

            print(f"Processing IMU data")

            # # Process accelerometer data
            # for i, acc in enumerate(imu_data.get("ArrayAcc", [])):
            #     point = (
            #         Point(measurement)
            #         .tag("Movesense_series", imu_data.get("Movesense_series", "unknown"))
            #         .field(f"Acc_x_{i}", acc.get("x", 0))
            #         .field(f"Acc_y_{i}", acc.get("y", 0))
            #         .field(f"Acc_z_{i}", acc.get("z", 0))
            #         .field("Timestamp_UTC", imu_data.get("Timestamp_UTC", int(time.time())))
            #         .field("Timestamp_ms", imu_data.get("Timestamp_ms", 0))
            #     )
            #     influx_client.write(database=database, record=point)
            #
            # # Process gyroscope data
            # for i, gyro in enumerate(imu_data.get("ArrayGyro", [])):
            #     point = (
            #         Point(measurement)
            #         .tag("Movesense_series", imu_data.get("Movesense_series", "unknown"))
            #         .field(f"Gyro_x_{i}", gyro.get("x", 0))
            #         .field(f"Gyro_y_{i}", gyro.get("y", 0))
            #         .field(f"Gyro_z_{i}", gyro.get("z", 0))
            #         .field("Timestamp_UTC", imu_data.get("Timestamp_UTC", int(time.time())))
            #         .field("Timestamp_ms", imu_data.get("Timestamp_ms", 0))
            #     )
            #     influx_client.write(database=database, record=point)
            #
            # # Process magnetometer data
            # for i, magn in enumerate(imu_data.get("ArrayMagn", [])):
            #     point = (
            #         Point(measurement)
            #         .tag("Movesense_series", imu_data.get("Movesense_series", "unknown"))
            #         .field(f"Magn_x_{i}", magn.get("x", 0))
            #         .field(f"Magn_y_{i}", magn.get("y", 0))
            #         .field(f"Magn_z_{i}", magn.get("z", 0))
            #         .field("Timestamp_UTC", imu_data.get("Timestamp_UTC", int(time.time())))
            #         .field("Timestamp_ms", imu_data.get("Timestamp_ms", 0))
            #     )
            #     influx_client.write(database=database, record=point)
            #
            # print("IMU data stored in InfluxDB")

            # Format imu arrays as JSON

            serialized_acc = json.dumps(imu_data.get("ArrayAcc", []))
            serialized_gyro = json.dumps(imu_data.get("ArrayGyro", []))
            serialized_magn = json.dumps(imu_data.get("ArrayMagn", []))

            # Create a single point for all the IMU data
            point = (
                Point(measurement_imu)
                .tag("Movesense_series", imu_data.get("Movesense_series", "unknown"))
                .field("ArrayAcc", serialized_acc)
                .field("ArrayGyro", serialized_gyro)
                .field("ArrayMagn", serialized_magn)
                .field("Timestamp_UTC", imu_data.get("Timestamp_UTC", int(time.time())))
                .field("Timestamp_ms", imu_data.get("Timestamp_ms", 0))
            )

            # Write to InfluxDB
            influx_client.write(database=database, record=point)
            print("IMU data stored in Influx")

        except Exception as e:
            print(f"Error processing IMU data: {e}")

    def process_gnss_data(self, gnss_data):
        """Process and store GNSS data in InfluxDB"""
        try:
            if not gnss_data:
                print("Empty GNSS data received")
                return

            print(f"Processing GNSS data")

            # Create point for InfluxDB
            point = (
                Point(measurement_gnss)
                .tag("GNSS_sensor_ID", gnss_data.get("GNSS_sensor_ID", "unknown"))
                .field("Latitude", gnss_data.get("Latitude", 0))
                .field("Longitude", gnss_data.get("Longitude", 0))
                .field("Date", gnss_data.get("Date", ""))
            )

            # Write to InfluxDB
            influx_client.write(database=database, record=point)
            print("GNSS data stored in InfluxDB")

        except Exception as e:
            print(f"Error processing GNSS data: {e}")

    def start(self):
        """Start the MQTT subscriber"""
        try:
            # Connect to MQTT broker
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)

            # Start MQTT loop
            print("Starting MQTT subscriber...")
            self.mqtt_client.loop_forever()

        except KeyboardInterrupt:
            print("Subscriber stopped by user")
        except Exception as e:
            print(f"Error in subscriber: {e}")
        finally:
            # Clean up
            self.mqtt_client.disconnect()
            print("Subscriber shutdown complete")


if __name__ == "__main__":
    subscriber = MQTTSubscriber()
    subscriber.start()