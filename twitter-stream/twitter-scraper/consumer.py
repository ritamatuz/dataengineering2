from kafka import KafkaConsumer


if __name__ == '__main__':
    consumer = KafkaConsumer(bootstrap_servers='34.141.185.185:9092',
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=1000 * 60)
    print(consumer.topics())
    consumer.subscribe(topics=['avg_sentiment'])
    for msg in consumer:
        print(msg.value.decode("utf-8"))
