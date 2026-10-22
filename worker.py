import json
import logging
import threading
import time
from flask import Flask, render_template_string
from flask_socketio import SocketIO
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)
app.config['SECRET_KEY'] = 'snmp-telemetry-secret'

# Use standard threading mode to prevent WSGI server freezes
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

KAFKA_BROKER = 'localhost:9094'
KAFKA_TOPIC = 'snmp'

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cisco 7200 Live Telemetry Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
    <style>
        body { font-family: sans-serif; background-color: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
        .container { max-width: 1100px; margin: 0 auto; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 15px; margin-bottom: 25px; }
        .status-badge { padding: 6px 12px; border-radius: 20px; font-size: 14px; font-weight: bold; background-color: #334155; color: #94a3b8; }
        .status-connected { background-color: #166534; color: #4ade80; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 25px; }
        .card { background-color: #1e293b; border-radius: 10px; padding: 20px; border: 1px solid #334155; }
        .card-title { font-size: 14px; color: #94a3b8; margin-bottom: 8px; text-transform: uppercase; }
        .card-value { font-size: 28px; font-weight: bold; }
        .inbound { color: #38bdf8; }
        .outbound { color: #f43f5e; }
        .chart-card { background-color: #1e293b; border-radius: 10px; padding: 20px; border: 1px solid #334155; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1 style="margin: 0; font-size: 24px;">Router Telemetry Dashboard</h1>
                <p style="margin: 5px 0 0 0; color: #94a3b8; font-size: 14px;">
                    Device: <span id="device-ip">192.168.238.129</span> | Interface: <span id="interface-name">Fa1/1</span>
                </p>
            </div>
            <div id="status-badge" class="status-badge">Connecting...</div>
        </div>

        <div class="metrics-grid">
            <div class="card">
                <div class="card-title">Inbound Throughput</div>
                <div class="card-value inbound"><span id="inbound-val">0.00</span> <span style="font-size: 16px;">Mbps</span></div>
            </div>
            <div class="card">
                <div class="card-title">Outbound Throughput</div>
                <div class="card-value outbound"><span id="outbound-val">0.00</span> <span style="font-size: 16px;">Mbps</span></div>
            </div>
            <div class="card">
                <div class="card-title">Last Update</div>
                <div class="card-value" style="font-size: 18px; color: #cbd5e1;" id="last-timestamp">--:--:--</div>
            </div>
        </div>

        <div class="chart-card">
            <canvas id="telemetryChart" height="110"></canvas>
        </div>
    </div>

    <script>
        const socket = io();
        const statusBadge = document.getElementById('status-badge');

        socket.on('connect', () => {
            statusBadge.innerText = '● Live Stream Active';
            statusBadge.className = 'status-badge status-connected';
        });

        socket.on('disconnect', () => {
            statusBadge.innerText = 'Disconnected';
            statusBadge.className = 'status-badge';
        });

        const ctx = document.getElementById('telemetryChart').getContext('2d');
        const telemetryChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Inbound (Mbps)',
                        borderColor: '#38bdf8',
                        backgroundColor: 'rgba(56, 189, 248, 0.1)',
                        data: [],
                        fill: true,
                        tension: 0.3
                    },
                    {
                        label: 'Outbound (Mbps)',
                        borderColor: '#f43f5e',
                        backgroundColor: 'rgba(244, 63, 94, 0.1)',
                        data: [],
                        fill: true,
                        tension: 0.3
                    }
                ]
            },
            options: {
                responsive: true,
                scales: {
                    x: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } },
                    y: {
                        beginAtZero: true,
                        grid: { color: '#334155' },
                        ticks: { color: '#94a3b8' },
                        title: { display: true, text: 'Throughput (Mbps)', color: '#94a3b8' }
                    }
                },
                plugins: { legend: { labels: { color: '#f8fafc' } } }
            }
        });

        socket.on('telemetry_update', (data) => {
            document.getElementById('device-ip').innerText = data.device_ip;
            document.getElementById('interface-name').innerText = data.interface;
            document.getElementById('inbound-val').innerText = data.inbound_mbps.toFixed(2);
            document.getElementById('outbound-val').innerText = data.outbound_mbps.toFixed(2);
            
            const timeStr = new Date(data.timestamp * 1000).toLocaleTimeString();
            document.getElementById('last-timestamp').innerText = timeStr;

            if (telemetryChart.data.labels.length >= 15) {
                telemetryChart.data.labels.shift();
                telemetryChart.data.datasets[0].data.shift();
                telemetryChart.data.datasets[1].data.shift();
            }

            telemetryChart.data.labels.push(timeStr);
            telemetryChart.data.datasets[0].data.push(data.inbound_mbps);
            telemetryChart.data.datasets[1].data.push(data.outbound_mbps);
            telemetryChart.update();
        });
    </script>
</body>
</html>
"""

def kafka_consumer_worker():
    """Non-blocking background thread for reading Kafka payloads."""
    logging.info(f"Connecting to Kafka broker at {KAFKA_BROKER} on topic '{KAFKA_TOPIC}'...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='snmp-web-dashboard-group'
        )
        logging.info("Kafka non-blocking consumer thread active.")
        
        while True:
            # Non-blocking poll for records (timeout 1000ms)
            records = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in records.items():
                for msg in messages:
                    payload = msg.value
                    logging.info(f"Emitting telemetry to Web UI: {payload}")
                    socketio.emit('telemetry_update', payload)
            time.sleep(0.1) # Yield execution back to Flask HTTP thread
            
    except Exception as e:
        logging.error(f"Error in Kafka thread: {e}")

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

if __name__ == '__main__':
    # Start Kafka consumer as a daemon thread before launching Flask
    t = threading.Thread(target=kafka_consumer_worker, daemon=True)
    t.start()
    
    logging.info("Starting Web Dashboard on http://127.0.0.1:5000...")
    socketio.run(app, host='127.0.0.1', port=5000, debug=False)
