import os
import re
import sys
import time
import twint
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic


class TwitterPartyStreamer:
    def __init__(self):
        self.temp = "temp.csv"
        self.stream = "stream.csv"
        #self.stream = "/home/jovyan/data/stream.csv"
        self.resume = "resume.txt"
        self.stream_columns = ["id", "created_at", "user_id", "tweet"]
        self.producer = KafkaProducer(bootstrap_servers=os.getenv("VM_EXTERNAL_IP") + ':9092')
        self.queries = {
            "vvd": "VVD",
            "d66": "D66",
            "pvv": "PVV",
            "partijvoordevrijheid": "PVV",
            "cda": "CDA",
            "sp": "SP",
            "pvda": "PvdA",
            "groenlinks": "Groenlinks",
            "fvd": "FvD",
            "forumvoordemocratie": "FvD",
            "pvdd": "PvdD",
            "partijvoordedieren": "PvdD",
            "christenunie": "Christenunie",
            "volt": "Volt",
            "ja21": "JA21",
            "sgp": "SGP",
            "denk": "DENK",
            "50plus": "50Plus",
            "bbb": "BBB",
            "boerburgerbeweging": "BBB",
            "bij1": "Bij1"
        }

    def configure_twint(self):
        config = twint.Config()
        config.Limit = 300
        config.Lang = "nl"
        config.Count = True
        config.Store_csv = True
        config.Hide_output = True
        config.Output = self.temp
        config.Near = "Amsterdam"
        config.Resume = self.resume
        config.Search = " OR ".join(["#" + query for query in [x for x in self.queries.keys()]])
        config.Custom["tweet"] = self.stream_columns
        return config

    def ingest(self):
        print("Scraping twitter for the latest tweets...", file=sys.stderr)

        # Configure Twint scraper
        config = self.configure_twint()

        # Scrape party tweets and store in temporary file
        twint.run.Search(config)

    def get_last_streamed_tweet(self):
        try:
            with open(self.stream, "rb") as file:
                # Go to the end of the file before the last break-line
                file.seek(-2, os.SEEK_END)
                # Keep reading backward until you find the next break-line
                while file.read(1) != b'\n':
                    file.seek(-2, os.SEEK_CUR)
                return file.readline().decode().split(",")
        except (FileNotFoundError, OSError):
            with open(self.stream, "w") as file:
                file.write(",".join(self.stream_columns) + "\n")
            return []

    def process_raw_tweet(self, row):
        msgs = list()

        # Decompose the csv row into columns
        row = row.split(",")
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
            party = self.queries.get(hl)
            if party is not None:
                msgs.append(f"{tweet_id},{user_id},{timestamp},{party},{sentiment},{tweet}")
        return msgs

    def stream_to_topic(self, topic):
        # Retrieve the last streamed id
        last_tweet = self.get_last_streamed_tweet()
        last_id = 0 if not last_tweet else int(last_tweet[0])

        # Scrape party tweets and store in temporary file
        self.ingest()

        # Open the temporary file for reading
        with open(self.temp, "r", encoding="utf8") as f_temp:

            # Open the stream for appending
            with open(self.stream, "a", encoding="utf8") as f_stream:

                # Iterate over the scraped tweets in reversed order
                for i, row in enumerate(reversed(list(f_temp)[1:])):

                    # Produce new tweets to kafka and store in csv
                    if int(row.split(",")[0]) > last_id:
                        msgs = self.process_raw_tweet(row)
                        for msg in msgs:
                            self.producer.send(topic, bytes(msg, encoding='utf-8'))
                            print("Sending " + msg)
                            self.producer.flush(timeout=60)
                            f_stream.write(msg)


def create_topic(admin_client, topic):
    try:
        admin_client.create_topics(new_topics=[
            NewTopic(name=topic, num_partitions=1, replication_factor=1),
        ], validate_only=False)
        print(f"Topic {topic} created.", file=sys.stderr)
    except TopicAlreadyExistsError:
        print(f"Topics {topic} already exist.")


if __name__ == '__main__':
    print("Start streaming...", file=sys.stderr)
    try:
        print("Creating topics...", file=sys.stderr)
        admin_client = KafkaAdminClient(bootstrap_servers=os.getenv("VM_EXTERNAL_IP") + ':9092', client_id='DE2')
        create_topic(admin_client, "twitter_politics")
        create_topic(admin_client, "avg_sentiment")

        while True:
            streamer = TwitterPartyStreamer()
            streamer.stream_to_topic("twitter_politics")
            time.sleep(300)
    except KeyboardInterrupt:
        print("Streaming has been interrupted.", file=sys.stderr)
