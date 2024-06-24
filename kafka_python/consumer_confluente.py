from confluent_kafka import Consumer, KafkaException
import json

# Configurações do Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Endereço do servidor Kafka
    'group.id': 'meu-grupo',                # ID do grupo de consumidores
    'auto.offset.reset': 'earliest',        # Início da leitura desde o primeiro offset disponível
}

# Criação do Consumer
consumer = Consumer(**conf)

# Inscrição no tópico
topic = 'venda'
consumer.subscribe([topic])


# Consumo de mensagens
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fim da partição {msg.partition()}")
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            event = json.dumps(
                {
                    "payload": {
                        "topic": topic,
                        "value": msg.value().decode('utf-8'),
                        "offset": msg.offset()
                    }
                }
            )
            print(event)
except KeyboardInterrupt:
    pass
finally:
    # Fechar o Consumer
    consumer.close()
