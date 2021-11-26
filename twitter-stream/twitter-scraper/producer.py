import os
import sys
import time
from streamer import TwitterPartyStreamer
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic


def create_topic(admin_client, topic):
    try:
        admin_client.create_topics(new_topics=[
            NewTopic(name=topic, num_partitions=1, replication_factor=1),
        ], validate_only=False)
        print(f"Topic {topic} created.", file=sys.stderr)
    except TopicAlreadyExistsError:
        print(f"Topics {topic} already exist.")


if __name__ == '__main__':
    try:
        print("Streaming has been started.", file=sys.stderr)

        print("Creating topics...", file=sys.stderr)
        admin_client = KafkaAdminClient(bootstrap_servers=os.getenv("VM_EXTERNAL_IP") + ':9092', client_id='DE2')
        create_topic(admin_client, "twitter_politics")
        create_topic(admin_client, "avg_sentiment")

        while True:
            print("Scraping twitter for the latest tweets...", file=sys.stderr)

            streamer = TwitterPartyStreamer()
            streamer.ingest()

            producer = KafkaProducer(bootstrap_servers=os.getenv("VM_EXTERNAL_IP") + ':9092')

            with open("/home/jovyan/data/stream.csv") as f:
                lines = f.readlines()

            print("Producing tweets in Kafka's twitter_politics topic.", file=sys.stderr)

            for line in lines:
                producer.send("twitter_politics", bytes(line, encoding='utf-8'))
                print("Sending " + line)
                producer.flush(timeout=60)

            time.sleep(300)

    except KeyboardInterrupt:
        print("Streaming has been interrupted.", file=sys.stderr)
