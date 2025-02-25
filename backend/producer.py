from kafka import KafkaProducer
import json

#Set up the producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Change this to self Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send a few messages to the "test" topic
for i in range(1, 6):
    message = {"message_id": i, "content": f"Message {i}"}
    producer.send('python', message)
    print(f"Sent: {message}")

# We can also send messages like this in JSON

    # message = {
    #     "player_id": 1234,
    #     "heart_rate_bpm": 110,
    #     "position_x": 53.2734,
    #     "position_y": -7.3222,
    # }

# Close the producer connection
producer.close()