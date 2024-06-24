from kafka import KafkaConsumer

# Configurações do Consumer
consumer = KafkaConsumer(
    'venda',
    bootstrap_servers='localhost:9092',
    group_id='meu-grupo',
    auto_offset_reset='earliest'
)

# Consumo de mensagens
try:
    for message in consumer:
        print(f"Recebido: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
    print("Interrompido pelo usuário")
finally:
    consumer.close()
