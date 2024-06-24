from kafka import KafkaProducer

# Configurações do Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Envio de uma mensagem
topic = 'venda'
mensagem = 'David'

producer.send(topic, mensagem.encode('utf-8'))
producer.flush()

print("Mensagem enviada com sucesso!")
