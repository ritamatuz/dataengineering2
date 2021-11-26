from kafka import KafkaConsumer


if __name__ == '__main__':
    consumer = KafkaConsumer(bootstrap_servers='34.88.146.250:9092',
                             auto_offset_reset='earliest',
                             consumer_timeout_ms=10000)
    print(consumer.topics())
    consumer.subscribe(topics=['count'])
    for msg in consumer:
        print(msg.value.decode("utf-8"))