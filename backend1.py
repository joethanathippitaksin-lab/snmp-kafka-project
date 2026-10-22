import json
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
TOPIC = "cisco-commands"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, indent=2).encode("utf-8"),
)

message = {
    "target_router": "192.168.238.129",
    "command": "show ip interface brief",
    "requested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
}

print("\n==================================================")
print(" 📤 [BACKEND 1] PUBLISHING COMMAND TO KAFKA")
print("==================================================")
print(f" Target Router : {message['target_router']}")
print(f" Command       : {message['command']}")
print(f" Kafka Topic   : {TOPIC}")
print("--------------------------------------------------")

producer.send(TOPIC, value=message)
producer.flush()

print(" STATUS        : Sent Successfully! ✅\n")
