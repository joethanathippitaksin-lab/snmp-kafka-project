import subprocess
import re
import time
import json
from kafka import KafkaProducer

IP = "192.168.238.129"
COMMUNITY = "public"
INTERVAL = 2         # Polling interval in seconds
DURATION = 60        # Total time to run in seconds
KAFKA_SERVER = "localhost:9094"
TOPIC_NAME = "snmp"

# Interface Index 3 maps to FastEthernet1/1
TARGET_OIDS = {
    "Inbound": "1.3.6.1.2.1.2.2.1.10.3",
    "Outbound": "1.3.6.1.2.1.2.2.1.16.3"
}

# Initialize Kafka Producer with JSON serializer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Connected to Kafka broker at {KAFKA_SERVER}")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    exit(1)

def get_interface_traffic(ip, community, oid):
    command = ["snmpwalk", "-v2c", "-c", community, "-On", ip, oid]
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode == 0:
        match = re.search(r'(?:Counter32|Counter64|INTEGER):\s*(\d+)', result.stdout)
        if match:
            return int(match.group(1))
    return None

print(f"Monitoring live bandwidth on {IP} (Fa1/1) and publishing to Kafka topic '{TOPIC_NAME}'...")

# Take initial baseline poll
previous_poll = {}
for direction, oid in TARGET_OIDS.items():
    previous_poll[direction] = get_interface_traffic(IP, COMMUNITY, oid)

iterations = DURATION // INTERVAL

for _ in range(iterations):
    time.sleep(INTERVAL)
    
    payload = {
        "device_ip": IP,
        "interface": "Fa1/1",
        "timestamp": time.time()
    }
    
    for direction, oid in TARGET_OIDS.items():
        current_poll = get_interface_traffic(IP, COMMUNITY, oid)
        
        if previous_poll.get(direction) is not None and current_poll is not None:
            delta_bytes = current_poll - previous_poll[direction]
            
            # Handle potential 32-bit SNMP counter wrap-around
            if delta_bytes < 0:
                delta_bytes += 4294967296 
            
            # Convert bytes to bits, divide by interval to get bps, convert to Mbps
            bits_per_second = (delta_bytes * 8) / INTERVAL
            mbps = bits_per_second / 1_000_000
            
            payload[f"{direction.lower()}_mbps"] = round(mbps, 2)
            previous_poll[direction] = current_poll
        else:
            payload[f"{direction.lower()}_mbps"] = None

    # Publish JSON payload to Kafka
    producer.send(TOPIC_NAME, value=payload)
    producer.flush()
    print(f"Published to Kafka: {payload}")
    print("-" * 50)
