import json
import os
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER")   # e.g. "1.2.3.4:9092"
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "iot-events")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    request_timeout_ms=10000,
)

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()
    return {"statusCode": 200, "body": "Message forwarded to Kafka"}
