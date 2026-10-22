import json
import time
from datetime import datetime
import paramiko
from kafka import KafkaConsumer, KafkaProducer

# Router Credentials
ROUTER_USER = "admin"
ROUTER_PASS = "cisco"
ROUTER_PORT = 22

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
INPUT_TOPIC = "cisco-commands"
OUTPUT_TOPIC = "cisco-responses"
CONSUMER_GROUP = "cisco-worker-group"


def clean_output(raw_text):
    """Strips terminal pagination noise (\r) and prompt setup commands."""
    lines = raw_text.replace("\r", "").splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if not stripped or "terminal length 0" in stripped:
            continue
        cleaned.append(stripped)
    return "\n".join(cleaned)


def execute_paramiko_command(ip, username, password, command):
    """Connects to Cisco router via Paramiko and executes CLI command."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(
            hostname=ip,
            port=ROUTER_PORT,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False,
            timeout=15,
            disabled_algorithms=dict(pubkeys=[], kex=[], ciphers=[]),
        )

        shell = ssh.invoke_shell()
        time.sleep(1)

        # Disable output pagination (--More--)
        shell.send("terminal length 0\n")
        time.sleep(0.5)

        if shell.recv_ready():
            shell.recv(65535)

        shell.send(command + "\n")
        time.sleep(2)

        raw_output = ""
        while shell.recv_ready():
            raw_output += shell.recv(65535).decode("utf-8", errors="ignore")

        ssh.close()
        return clean_output(raw_output), "SUCCESS"

    except Exception as e:
        return f"SSH Error: {str(e)}", "FAILED"


def start_worker():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, indent=2).encode("utf-8"),
    )

    print("\n==================================================")
    print(" 🚀 [BACKEND 2 WORKER] ONLINE AND LISTENING")
    print(f" Consumer Group: {CONSUMER_GROUP}")
    print(f" Input Topic   : {INPUT_TOPIC}")
    print("==================================================\n")

    for message in consumer:
        payload = message.value
        target_ip = payload.get("target_router", "192.168.238.129")
        command = payload.get("command", "show ip interface brief")

        print("--------------------------------------------------")
        print(f" 📩 [TASK RECEIVED] {datetime.now().strftime('%H:%M:%S')}")
        print(f" Target Router : {target_ip}")
        print(f" Command       : {command}")
        print(" Connecting to router via Paramiko...")

        cli_output, status = execute_paramiko_command(
            target_ip, ROUTER_USER, ROUTER_PASS, command
        )

        response_payload = {
            "status": status,
            "router_ip": target_ip,
            "command": command,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "output": cli_output,
        }

        producer.send(OUTPUT_TOPIC, value=response_payload)
        producer.flush()

        print(f" Status        : {status} ✅")
        print(f" Output Sent To: {OUTPUT_TOPIC}")
        print("--------------------------------------------------\n")


if __name__ == "__main__":
    start_worker()
