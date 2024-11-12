import csv
import random
import time
from kafka import KafkaProducer

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

# Nama topik Kafka
topic_name = 'iot_data'

# Membaca file CSV
with open('IOT-temp.csv', mode='r') as file:
    csv_reader = csv.reader(file)
    headers = next(csv_reader)  # Menyimpan header untuk digunakan nanti di consumer

    # Mengirim data baris demi baris dengan delay random
    for row in csv_reader:
        # Gabungkan setiap kolom dengan koma dan kirim ke Kafka
        producer.send(topic_name, value=','.join(row))
        print(f"Data sent: {row}")
        
        # Delay random antara 1 hingga 3 detik
        time.sleep(random.randint(1, 3))

producer.flush()
producer.close()
