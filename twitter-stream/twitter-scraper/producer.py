import os
import sys
import time
from streamer import TwitterPartyStreamer
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


if __name__ == '__main__':
    try:
        print("Streaming has been started.", file=sys.stderr)

        print("Creating topics...", file=sys.stderr)
        admin_client = KafkaAdminClient(bootstrap_servers=os.getenv("VM_EXTERNAL_IP") + ':9092', client_id='DE2')
        admin_client.create_topics(new_topics=[
            NewTopic(name="twitter_politics", num_partitions=1, replication_factor=1),
            NewTopic(name="avg_sentiment", num_partitions=1, replication_factor=1)
        ], validate_only=False)
        print("Topics created.", file=sys.stderr)

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
