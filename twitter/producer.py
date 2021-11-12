import os
from kafka import KafkaProducer


def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)


if __name__ == '__main__':
    # Change to external IP of VM
    producer = KafkaProducer(bootstrap_servers='34.88.146.250:9092')

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "stream.csv")) as f:
        lines = f.readlines()

    for line in lines:
        kafka_python_producer_sync(producer, line, "twitter_politics")
