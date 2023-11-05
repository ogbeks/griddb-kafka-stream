from kafka import KafkaConsumer
import griddb_python
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BROKER = 'kafka:9092'
TOPIC_NAME = 'YOUR_TOPIC_NAME'
GRIDDB_CONFIG = {
    "notification_member": "griddb:10001",
    "cluster_name": "your_cluster_name",
    "username": "your_username",
    "password": "your_password"
}

def save_to_griddb(timestamp, state, aqi):
    try:
        factory = griddb.StoreFactory.get_default()
        gridstore = factory.get_store(host=GRIDDB_CONFIG["notification_member"],
                                      cluster_name=GRIDDB_CONFIG["cluster_name"],
                                      username=GRIDDB_CONFIG["username"],
                                      password=GRIDDB_CONFIG["password"])

        conInfo = griddb.ContainerInfo("AirQuality",
                                       [["timestamp", griddb.Type.STRING],
                                        ["state", griddb.Type.STRING],
                                        ["aqi", griddb.Type.INTEGER]],
                                       griddb.ContainerType.COLLECTION, True)
                                       
        cont = gridstore.put_container(conInfo)
        cont.set_auto_commit(False)
        
        cont.put([[timestamp, state, aqi]])
        
        cont.commit()
        logger.info(f"Saved data to GridDB: Timestamp: {timestamp}, State: {state}, AQI: {aqi}")

    except Exception as e:
        logger.error(f"An error occurred with GridDB: {e}")

def consume():
    consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=BROKER)
    logger.info(f"Started Kafka Consumer, listening on topic: {TOPIC_NAME}")

    for message in consumer:
        data = message.value.decode('utf-8')
        timestamp, state, aqi = data.split("\t")
        logger.info(f"Received message from Kafka: Timestamp: {timestamp}, State: {state}, AQI: {aqi.strip()}")
        save_to_griddb(timestamp, state, int(aqi.strip()))

if __name__ == "__main__":
    consume()
