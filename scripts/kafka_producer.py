"""
Step 2: Read logs from file and send to Kafka
This script tails the log file and sends each new line to Kafka
"""
import time
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
KAFKA_BROKER = "localhost:29092"
TOPIC = "raw-logs"
script_dir = Path(__file__).parent.parent
LOG_FILE = script_dir / "logs" / "system.log"

print("=" * 60)
print("📤 KAFKA PRODUCER - Send logs to Kafka")
print("=" * 60)
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Topic: {TOPIC}")
print(f"Reading from: {LOG_FILE}")
print("=" * 60 + "\n")

# Create Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: v.encode('utf-8'),
        acks='all',  # Wait for all replicas to acknowledge
        retries=3
    )
    print("✅ Connected to Kafka broker\n")
except KafkaError as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    print("Make sure Docker containers are running: docker-compose ps")
    exit(1)

# Function to tail file (read new lines as they appear)
def tail_file(file_path):
    """Read new lines from file as they are written"""
    with open(file_path, 'r', encoding='utf-8') as f:
        # Go to end of file
        f.seek(0, 2)
        
        while True:
            line = f.readline()
            if line:
                yield line.strip()
            else:
                time.sleep(0.5)  # Wait for new data

# Send logs to Kafka
count = 0
print("🔄 Waiting for new log entries...\n")

try:
    for log_line in tail_file(LOG_FILE):
        # Send to Kafka
        future = producer.send(TOPIC, value=log_line)
        
        try:
            # Wait for send to complete (with timeout)
            record_metadata = future.get(timeout=10)
            count += 1
            
            # Print status every 10 messages
            if count % 10 == 0:
                print(f"✅ Sent {count} logs | Latest: {log_line[:80]}...")
                
        except KafkaError as e:
            print(f"❌ Failed to send log: {e}")

except KeyboardInterrupt:
    print(f"\n\n🛑 Stopping producer. Total logs sent: {count}")
finally:
    producer.flush()
    producer.close()
    print("✅ Producer closed gracefully")