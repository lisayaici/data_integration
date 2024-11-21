from kafka import KafkaConsumer
import json

def start_consumer():
    # Initialiser le Kafka Consumer
    consumer = KafkaConsumer(
        'data-topic', 
        bootstrap_servers='localhost:9092', 
        group_id='data-group',  
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
    )
    print("Démarrage du consumer...")
    print("Consumer connecté à Kafka, en attente de messages...")

    # Consommer les messages
    for message in consumer:
        data = message.value 
        print(f"Reçu depuis Kafka: {data}")
        print(f"Traitement des données : {data}")
if __name__ == "__main__":
    start_consumer()

