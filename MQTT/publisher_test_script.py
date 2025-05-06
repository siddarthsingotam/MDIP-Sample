import random
from datetime import datetime

import paho.mqtt.client as mqtt
import time
import json

# MQTT Configuration
MQTT_CONFIG = {
    "server": b"51.21.239.39",
    "port": 1883,
    "username": b"iotuser",
    "password": b"iotuser2025",
}

# Topics to publish to
TOPICS = [
    "sensors/imu",
    "sensors/hr",
    "sensors/gnss",
    "sensors/ecg"
]

PICO_ID = "e66130100f8c9928"

# Sample data for publishing
SAMPLE_DATA = {
    "sensors/imu": {
        'Pico_ID': PICO_ID,
        'Movesense_series': "174630000192",
        'Timestamp_UTC': int(time.time()),
        'Timestamp_ms': 30348,
        'ArrayAcc': [
            {'x': 8.198, 'y': -5.47, 'z': 1.235},
            {'x': 8.279, 'y': -5.37, 'z': 1.278},
            {'x': 8.126, 'y': -5.296, 'z': 1.251},
            {'x': 7.935, 'y': -5.202, 'z': 1.232}
        ],
        "ArrayGyro": [
            {'x': 2.8, 'y': 6.3, 'z': 5.6},
            {'x': 0.56, 'y': 9.52, 'z': 8.82},
            {'x': -1.75, 'y': 7.0, 'z': 11.2},
            {'x': 1.68, 'y': -0.63, 'z': 11.06}
        ],
        "ArrayMagn": [
            {'x': 42.6, 'y': -28.5, 'z': 3.45},
            {'x': 44.1, 'y': -27.45, 'z': 5.7},
            {'x': 43.35, 'y': -28.2, 'z': 4.05},
            {'x': 43.2, 'y': -25.05, 'z': 3.3}
        ]
    },
    "sensors/hr": {
        'rrData': [384],
        'Pico_ID': PICO_ID,
        'Movesense_series': '174630000192',
        'Timestamp_UTC': int(time.time()),
        'average_bpm': 108.7813
    },
    "sensors/gnss": {
        'Pico_ID': PICO_ID,
        'GNSS_ID': 'device123',
        'Date': datetime.now().isoformat(),
        'Latitude': 37.7749 + random.uniform(-0.01, 0.01),
        'Longitude': -122.4194 + random.uniform(-0.01, 0.01)
    },
    "sensors/ecg": {
        'Pico_ID': PICO_ID,
        'Samples': [-62342, -51680, -43311, -35942, -29149, -23343, -18437, -14248, -10712, -7628, -4838, -2323,
                    -90, 1953, 3926, 5939],
        'Movesense_series': '174630000192',
        'Timestamp_UTC': int(time.time()),
        'Timestamp_ms': 29889
    }
}

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker successfully!")
    else:
        print(f"Failed to connect, return code {rc}")

# Create MQTT client
client = mqtt.Client()
client.username_pw_set(MQTT_CONFIG["username"].decode(), MQTT_CONFIG["password"].decode())

# Assign callbacks
client.on_connect = on_connect

# Connect to the broker
try:
    print("Connecting to MQTT broker...")
    client.connect(MQTT_CONFIG["server"].decode(), MQTT_CONFIG["port"], 60)

    # Start publishing messages
    client.loop_start()
    while True:
        for topic in TOPICS:
            message = SAMPLE_DATA.get(topic, {"default": "No data"})
            client.publish(topic, json.dumps(message))
            print(f"Published to {topic}: {message}")
        time.sleep(5)  # Publish every 5 seconds

except KeyboardInterrupt:
    print("Publisher stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    client.loop_stop()
    client.disconnect()
    print("Publisher shutdown COMPLETE")