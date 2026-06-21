# kafka/create_topics.py
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv
import os

load_dotenv()

def create_topics():
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        client_id="mining-admin"
    )

    topics = [
        NewTopic(name="sensor-events",      num_partitions=6, replication_factor=1),
        NewTopic(name="equipment-alerts",   num_partitions=3, replication_factor=1),
        NewTopic(name="equipment-status",   num_partitions=3, replication_factor=1),
    ]

    existing = admin.list_topics()
    new_topics = [t for t in topics if t.name not in existing]

    if new_topics:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        print(f"Created topics: {[t.name for t in new_topics]}")
    else:
        print("All topics already exist")

    admin.close()

if __name__ == "__main__":
    create_topics()
