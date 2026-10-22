import json
import time
from datetime import datetime
import paramiko
from kafka import KafkaConsumer, KafkaProducer

ROUTER_IP = "192.168.238.129"
ROUTER_USER = "admin"
ROUTER_PASS = "cisco"
ROUTER_PORT = 22

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
INPUT_TOPIC = "cisco-commands"
OUTPUT_TOPIC = "cisco-responses"


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


def execute_cisco_command(ip, username, password, command):
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
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, indent=2).encode("utf-8"),
    )

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="cisco-worker-group",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("\n==================================================")
    print(" 🚀 [CISCO WORKER] ACTIVE AND LISTENING")
    print(f" Input Topic : '{INPUT_TOPIC}'")
    print("==================================================\n")

    for message in consumer:
        payload = message.value
        command = payload.get("command", "show ip interface brief")

        print("--------------------------------------------------")
        print(f" 📩 [COMMAND RECEIVED] {datetime.now().strftime('%H:%M:%S')}")
        print(f" Target Router : {ROUTER_IP}")
        print(f" Command       : {command}")

        cli_output, status = execute_cisco_command(
            ROUTER_IP, ROUTER_USER, ROUTER_PASS, command
        )

        response_data = {
            "status": status,
            "router_ip": ROUTER_IP,
            "command": command,
            "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "output": cli_output,
        }

        producer.send(OUTPUT_TOPIC, value=response_data)
        producer.flush()

        print(f" Status        : {status} ✅")
        print(f" Result Topic  : {OUTPUT_TOPIC}")
        print("--------------------------------------------------\n")


if __name__ == "__main__":
    start_worker()
