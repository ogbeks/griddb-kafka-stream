import time
import random
from kafka import KafkaProducer
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STATES = ["California", "Texas", "New York", "Florida", "Illinois"]

def generate_aqi():
    
    return random.randint(0, 500)

def simulate_iot_data(producer, topic_name):
    while True:
        for state in STATES:
            aqi = generate_aqi()
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            data = f"{timestamp}\t{state}\t\t{aqi}"
            producer.send(topic_name, value=data)
            logger.info(f"Sent data for {state}: {data}")

        producer.flush()
        logger.info("Flushed all outstanding messages")
        time.sleep(5)

if __name__ == "__main__":
    logger.info("Starting Kafka producer")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: str(v).encode('utf-8')
    )

    topic_name = 'YOUR_TOPIC_NAME'  # Replace with your Kafka topic name
    logger.info(f"Sending data to topic: {topic_name}")
    simulate_iot_data(producer, topic_name)
