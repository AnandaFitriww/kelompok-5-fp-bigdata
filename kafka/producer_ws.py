import json
import websocket
import threading
from kafka import KafkaProducer

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Topic names sesuai arsitektur lakehouse
TOPIC_TELEMETRY = "bus-telemetry-raw"
TOPIC_TRANSACTION = "transaction-raw"

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Skip pesan control (server info)
        if data.get("type") == "control":
            print(f"[SERVER INFO] {data.get('event')}")
            return

        # TELEMETRY: Explode batch → kirim per-record ke Kafka
        if data.get("type") == "vehicle_telemetry_batch":
            vehicles = data.get("vehicles", [])
            for vehicle in vehicles:
                producer.send(TOPIC_TELEMETRY, vehicle)
            print(f"[TELEMETRY] Berhasil kirim {len(vehicles)} record ke Kafka topic '{TOPIC_TELEMETRY}'.")
            
        # TRANSACTION: Explode batch → kirim per-record ke Kafka
        elif data.get("type") == "transaction_batch":
            transactions = data.get("transactions", [])
            for txn in transactions:
                producer.send(TOPIC_TRANSACTION, txn)
            print(f"[TRANSACTION] Berhasil kirim {len(transactions)} record ke Kafka topic '{TOPIC_TRANSACTION}'.")
            
    except Exception as e:
        print(f"Gagal memproses pesan: {e}")

def on_error(ws, error):
    print(f"[ERROR] {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"[CLOSED] Koneksi ke {ws.url} terputus.")

def on_open(ws):
    print(f"[OPEN] Terhubung ke {ws.url}. Mengirim perintah 'start'...")
    ws.send("start")  # WAJIB agar server mulai kirim data

def run_ws(url):
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    url_transaction = "ws://70.153.136.193:8765"
    url_telemetry = "ws://70.153.136.193:8766"

    print("Memulai Dual-Stream WebSocket ke Kafka...")
    print(f"  Topic telemetry : {TOPIC_TELEMETRY}")
    print(f"  Topic transaksi : {TOPIC_TRANSACTION}")
    
    # Menjalankan 2 WebSocket secara paralel dengan Threading
    t1 = threading.Thread(target=run_ws, args=(url_transaction,), daemon=True)
    t2 = threading.Thread(target=run_ws, args=(url_telemetry,), daemon=True)
    
    t1.start()
    t2.start()
    
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\nDihentikan manual oleh user.")
    finally:
        producer.flush()