# mqtt_subscriber.py
import json
import time
import os
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3, Point

from playerData.db_manager import DatabaseManager

# Load environment variables for InfluxDB credentials
KEY_PATH = r"..\keys\keys.env"
load_dotenv(KEY_PATH)

#token = os.getenv("ALL_ACCESS_TOKEN")
token = "ZszZ4Lyn1uYBdOelwshQu9eR2c81M71AuBo1dtakyrcNoVqv32p6Cxjji2BCOa8HKwZ7nxkxTwLspKaNxeDF_Q=="
if not token:
    raise ValueError("Environment variable ALL_ACCESS_TOKEN is not set.")

# InfluxDB Configuration
org = "MDIP-Sample"
host = "https://eu-central-1-1.aws.cloud2.influxdata.com"
database = "Full_Player_Data"


# Setting up all the measurements for the data to be logged in influx
measurement_hr = "measurement_heart_rate"
measurement_ecg = "measurement_ecg"
measurement_imu = "measurement_imu"
measurement_gnss = "measurement_gnss"
t1_payload_key = "publish_time_ms"

# Initialize InfluxDB client
influx_client = InfluxDBClient3(host=host, token=token, org=org)

# Setup Database Manager
DB = DatabaseManager()
try:
    if DB:
        print("Connected to local player DB in Database manager...")
except NotImplementedError as e:
    print(f"Error in DatabaseManager class {e}")


class MQTTSubscriber:
    def __init__(self, hive_mq=False):
        """Initiation on connecting default to the own config broker, else hiveMQ on specification"""
        self.hive_mq = hive_mq
        if self.hive_mq:
            # Hive MQTT Configuration
            self.mqtt_broker = "d4e877f7c282469c87fe4307599ad40c.s1.eu.hivemq.cloud"
            self.mqtt_port = 8883  # TLS port
            self.mqtt_username = "AtomBerg1"  # Update with your username
            self.mqtt_password = "AtomBerg1"  # Update with your password
            self.mqtt_client_id = f"sensor-subscriber-XXXX"
        else:
            # MQTT Configuration on Local
            self.mqtt_broker = b"51.21.239.39"
            self.mqtt_port = 1883
            self.mqtt_username = b"iotuser"
            self.mqtt_password = b"iotuser2025"
            self.mqtt_client_id = f"sensor-subscriber-XXXX"

        # MQTT Topics to subscribe to
        self.topics = [
            "sensors/imu",
            "sensors/ecg",
            "sensors/hr",
            "sensors/gnss"
        ]

        # Initialize MQTT client
        try:
            if self.hive_mq:
                print(f"Initializing HiveMQ client...")
                self.mqtt_client = mqtt.Client(client_id=self.mqtt_client_id, protocol=mqtt.MQTTv5)
                self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)
                self.mqtt_client.tls_set()  # Enable TLS for HiveMQ Cloud!!!!
            else:
                print(f"Initializing custom MQTT client...")
                self.mqtt_client = mqtt.Client()
                self.mqtt_client.username_pw_set(self.mqtt_username.decode(), self.mqtt_password.decode())
        except ConnectionError as e:
            print(f"ERROR: Unable to connect MQTT, reason: {e}")

        # Connect MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected to MQTT broker!")

            # Subscribe to all sensor topics
            for topic in self.topics:
                client.subscribe(topic, qos=1)  # deliver message least once
                print(f"Subscribed to topic: {topic}")
            print(100 * "-")
        else:
            print(f"Failed to connect to MQTT broker with code {rc}")

    def on_message(self, client, userdata, msg):
        print(100 * "-")
        print(f"Received message on topic {msg.topic}")
        try:
            # Parse JSON payload
            payload = json.loads(msg.payload.decode())


            # Extract pico_id from payload
            pico_id = None
            if "Pico_ID" in payload:
                pico_id = payload.get("Pico_ID")

            if pico_id:
                print(f"Extracted Pico_ID: {pico_id}")
            else:
                print("Pico_ID not found in the payload")

            try:
                # Extract time t1 from payload (in milliseconds)
                t1 = payload.get(t1_payload_key, None)
                # Now recording current time t2 (in milliseconds as well)
                t2 = time.time_ns() // 1_000_000
                # Now calculate latency
                latency = t2 - t1
                print(f"Latency for Pico: {pico_id}, on topic {msg.topic}: {latency} ms")
            except NotImplementedError:
                pass

            # Process the message based on the topic
            if msg.topic == "sensors/hr":
                # Handle heart rate data
                self.process_hr_data(payload, pico_id)
            elif msg.topic == "sensors/ecg":
                # Handle ECG data
                self.process_ecg_data(payload, pico_id)
            elif msg.topic == "sensors/imu":
                # Handle IMU data
                self.process_imu_data(payload, pico_id)
            elif msg.topic == "sensors/gnss":
                # Handle GNSS data
                self.process_gnss_data(payload, pico_id)

        except Exception as e:
            print(f"Error processing message: {e}")

    def process_hr_data(self, hr_data, pico_id):
        """Process and store heart rate data in InfluxDB"""
        try:
            if not hr_data:
                print("Empty HR data received")
                return

            print(f"Processing HR data: {hr_data}")

            # Serialize rrData as JSON
            serialized_rrData = json.dumps({"data": hr_data.get("rrData", [])})

            # Get Pico Information from the DB
            player_data = DB.get_player_by_pico_id(pico_id=pico_id)
            print(f"Player data: {player_data}")

            # Create point for InfluxDB
            point = (
                Point(measurement_hr)
                .tag("Pico_ID", player_data["pico_id"])
                .tag("Player_ID", player_data["player_id"])
                .field("Player Name", player_data["name"])
                .field("Movesense_series", hr_data.get("Movesense_series", "unknown"))
                .field("rrData", serialized_rrData)
                .field("Timestamp_UTC", hr_data.get("Timestamp_UTC", int(time.time())))
                .field("average_bpm", hr_data.get("average_bpm", 0))
            )

            print(f"Processing HR data: {hr_data}")
            # Write to InfluxDB
            influx_client.write(database=database, record=point)
            print("HR data stored in InfluxDB")

        except Exception as e:
            print(f"Error processing HR data: {e}")

    def process_ecg_data(self, ecg_data, pico_id):
        """Process and store ECG data in InfluxDB"""
        try:
            if not ecg_data:
                print("Empty ECG data received")
                return

            print(f"Processing ECG data: {ecg_data}")

            # Serialize samples as JSON
            serialized_samples = json.dumps({"data": ecg_data.get("Samples", [])})

            # Get Pico Information from the DB
            player_data = DB.get_player_by_pico_id(pico_id=pico_id)
            print(f"Player data: {player_data}")

            # Create point for InfluxDB
            point = (
                Point(measurement_ecg)
                .tag("Pico_ID", player_data["pico_id"])
                .tag("Player_ID", player_data["player_id"])
                .field("Movesense_series", ecg_data.get("Movesense_series", "unknown"))
                .field("Samples", serialized_samples)
                .field("Timestamp_UTC", ecg_data.get("Timestamp_UTC", int(time.time())))
                .field("Timestamp_ms", ecg_data.get("Timestamp_ms", 0))
            )

            # Write to InfluxDB
            influx_client.write(database=database, record=point)
            print("ECG data stored in InfluxDB")

        except Exception as e:
            print(f"Error processing ECG data: {e}")

    def process_imu_data(self, imu_data, pico_id):
        """Process and store IMU data in InfluxDB"""
        try:
            if not imu_data:
                print("Empty IMU data received")
                return

            print(f"Processing IMU data: {imu_data}")

            # Format imu arrays as JSON
            serialized_acc = json.dumps(imu_data.get("ArrayAcc", []))
            serialized_gyro = json.dumps(imu_data.get("ArrayGyro", []))
            serialized_magn = json.dumps(imu_data.get("ArrayMagn", []))

            # Get Pico Information from the DB
            player_data = DB.get_player_by_pico_id(pico_id=pico_id)
            print(f"Player data: {player_data}")

            # Create a single point for all the IMU data
            point = (
                Point(measurement_imu)
                .tag("Pico_ID", player_data["pico_id"])
                .tag("Player_ID", player_data["player_id"])
                .field("Movesense_series", imu_data.get("Movesense_series", "unknown"))
                .field("ArrayAcc", serialized_acc)
                .field("ArrayGyro", serialized_gyro)
                .field("ArrayMagn", serialized_magn)
                .field("Timestamp_UTC", imu_data.get("Timestamp_UTC", int(time.time())))
                .field("Timestamp_ms", imu_data.get("Timestamp_ms", 0))
            )

            # Write to InfluxDB
            influx_client.write(database=database, record=point)
            print("IMU data stored in InfluxDB")

        except Exception as e:
            print(f"Error processing IMU data: {e}")

    def process_gnss_data(self, gnss_data, pico_id):
        """Process and store GNSS data in InfluxDB"""
        try:
            if not gnss_data:
                print("Empty GNSS data received")
                return

            print(f"Processing GNSS data: {gnss_data}")

            # Get Pico Information from the DB
            player_data = DB.get_player_by_pico_id(pico_id=pico_id)
            print(f"Player data: {player_data}")

            # Create point for InfluxDB
            point = (
                Point(measurement_gnss)
                .tag("Pico_ID", player_data["pico_id"])
                .tag("Player_ID", player_data["player_id"])
                .field("GNSS_sensor_ID", gnss_data.get("GNSS_sensor_ID", "unknown"))
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
            if self.hive_mq:
                # Connect to MQTT broker
                self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
                print("Connected to HiveMQ client!")
            else:
                self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port)
                print("Connected to custom custom MQTT client!")

            # Start MQTT loop
            print("Starting MQTT subscriber...")
            self.mqtt_client.loop_forever()

        except KeyboardInterrupt:
            print(100 * "*")
            print("Subscriber stopped by user")
        except Exception as e:
            print(f"Error in subscriber: {e}")
        finally:
            # Clean up
            self.mqtt_client.disconnect()
            print("Subscriber shutdown COMPLETE")


if __name__ == "__main__":
    subscriber = MQTTSubscriber(hive_mq=False)
    subscriber.start()
