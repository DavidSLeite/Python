from kafka import KafkaConsumer as kc

consumidor = kc(bootstrap_servers='127.0.0.1:9092', consumer_timeout_ms=1000, group_id='consumidores')

for msg in consumidor:
    print("Topic: ", msg.topic)
    print("Partition: ", msg.partition)
    print("Key: ", msg.key)
    print("Offset: ", msg.offset)
    print("Mensagem: ", msg.value)
    print("----------------------------------")