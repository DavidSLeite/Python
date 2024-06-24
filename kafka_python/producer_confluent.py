from confluent_kafka import Producer
import json

event = [
    {
        "ID": 1,
        "TELEFONES":[
            {"ddd": "11", "numero": None},
            {"ddd": "12", "numero": "40028922"},
        ]
    },
    {
        "ID": 2,
        "TELEFONES":[
            {"ddd": "88", "numero": "1234871"},
            {"ddd": "12", "numero": "121681"},
        ]
    }
]

def delivery_report(err, msg):
    """ Callback called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Configuração do produtor
conf = {
    'bootstrap.servers': 'localhost:9092',  # Endereço do broker Kafka
}

producer = Producer(**conf)

# Produzindo uma mensagem
topic = 'venda'
producer.produce(topic, key='key', value=json.dumps(event), callback=delivery_report)

# Aguarda que todas as mensagens sejam entregues
producer.flush()
