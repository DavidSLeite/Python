from kafka import KafkaProducer as kp
import random

produtor = kp(bootstrap_servers='localhost:9092')
topic = 'mensagens'

for x in range(10):
    n = random.random()
    produtor.send(topic, key=b"Chave %d" % x, value=b"{\"fruta\":\"banana\", \"preco\":%f}" % n)
