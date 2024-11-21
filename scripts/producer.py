from kafka import KafkaProducer
import pandas as pd
import json
import time

def start_producer():
    # Charger les données nettoyées depuis le répertoire 'processed'
    LTM_data_cleaned = pd.read_csv("/Users/lisayaici/Desktop/Projet_data_integration/data/processed/LTM_Data_cleaned.csv")  # Change le chemin si nécessaire

    # Initialiser le Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  
    )

    print("Démarrage du producer...")
    
    # Diviser les données en lots de 10
    for i in range(0, len(LTM_data_cleaned), 10):
        data_batch = LTM_data_cleaned.iloc[i:i+10]  
        data_batch_dict = data_batch.to_dict(orient='records')  
        
        # Envoyer chaque lot au topic Kafka
        producer.send('data-topic', value=data_batch_dict)
        print(f"Envoyé au Kafka topic 'data-topic': {data_batch_dict}")

        # Attendre 10 secondes avant d'envoyer le prochain lot
        time.sleep(10)


    producer.flush() 
    producer.close()

if __name__ == "__main__":
    start_producer()
