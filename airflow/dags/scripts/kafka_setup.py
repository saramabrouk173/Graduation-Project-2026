from kafka.admin import KafkaAdminClient, NewTopic

KAFKA_BOOTSTRAP_SERVER = "kafka-broker-1:9092"
TOPIC_NAME = "water_readings"

def create_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        client_id="ewis_admin"
    )

    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=1,
        replication_factor=1
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✅ Topic '{TOPIC_NAME}' created successfully.")
    except Exception as e:
        print(f"⚠️ Topic may already exist or error occurred: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_topic()