import os
import re
import sys
import time
import requests
from datetime import datetime
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


def produce_to_topic(producer, topic, msg):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


def process_raw_tweet(line, queries):
    msgs = list()

    # Decompose the csv line into columns
    row = line.split(",")
    tweet_id = row[0]
    timestamp = datetime.timestamp(datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S %Z"))
    user_id = row[2]
    tweet = "".join(row[3:]).replace(",", "").replace("'", "").replace('"', '')
    hashtags = set([re.sub(r"(\W+)$", "", j) for j in set([i.replace("#", "") for i in tweet.split() if i.startswith("#")])])

    # Compute the sentiment
    sentiment = requests.post('http://' + os.getenv("VM_EXTERNAL_IP") + ":5000/sentiment-api/analyze",
                              json={"tweet": tweet}).json()['sentiment_score']

    # Add a message for each party hashtag
    for hashtag in hashtags:
        hl = hashtag.lower()
        party = queries.get(hl)
        if party is not None:
            msgs.append(f"{tweet_id},{user_id},{timestamp},{party},{sentiment},{tweet}")
    return msgs


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

            first_line = True
            for line in lines:
                if first_line:
                    first_line = False
                    continue
                msgs = process_raw_tweet(line, streamer.queries)
                for msg in msgs:
                    produce_to_topic(producer, "twitter_politics", msg)

            time.sleep(300)

    except KeyboardInterrupt:
        print("Streaming has been interrupted.", file=sys.stderr)
