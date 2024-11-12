import csv
import time
from kafka import KafkaConsumer

# Kafka consumer configuration
consumer = KafkaConsumer('iot_data', bootstrap_servers='localhost:9092', group_id='iot_group', auto_offset_reset='earliest', value_deserializer=lambda x: x.decode('utf-8'))

# Counter untuk nomor batch
batch_counter = 1

# Fungsi untuk menghasilkan nama file batch baru
def generate_batch_filename():
    global batch_counter
    filename = f"batch_{batch_counter}.csv"
    batch_counter += 1
    return filename

# Waktu batch dalam detik (misalnya 30 detik)
batch_interval = 300
batch = []
start_time = time.time()
batch_filename = generate_batch_filename()

# Looping membaca pesan dari Kafka dan menulis batch setiap interval waktu
while True:
    # Membuka file batch baru untuk ditulisi
    with open(batch_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Tulis header pada file batch
        writer.writerow(['id', 'room_id/id', 'noted_date', 'temp', 'out/in'])
        
        # Proses setiap pesan dari Kafka
        for message in consumer:
            # Tambahkan pesan ke batch
            batch.append(message.value.split(','))
            
            # Jika sudah mencapai interval batch (misalnya 30 detik)
            if time.time() - start_time >= batch_interval:
                # Tulis batch ke dalam file CSV
                writer.writerows(batch)
                print(f"Batch written: {batch_filename}")
                
                # Reset batch dan waktu
                batch = []
                start_time = time.time()
                batch_filename = generate_batch_filename()  # Nama file baru untuk batch berikutnya
                break  # Keluar dari loop for untuk membuka file baru pada batch berikutnya
            
            time.sleep(1)  # Memberikan jeda agar consumer tidak overload
