import json
import websocket
import threading
from kafka import KafkaProducer

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # Pengecekan respons awal (server info)
        if data.get("type") == "control":
            print(f"[SERVER INFO] {data.get('event')}")
            return

        # PISAHKAN JALUR DATA: Penumpang vs GPS
        if data.get("type") == "transaction_batch":
            producer.send('topic-penumpang', data)
            print(f"[PENUMPANG] Berhasil kirim {data.get('count')} data transaksi ke Kafka.")
            
        elif data.get("type") == "vehicle_telemetry_batch":
            producer.send('topic-gps', data)
            print(f"[GPS] Berhasil kirim {data.get('count')} data posisi bus ke Kafka.")
            
    except Exception as e:
        print(f"Gagal memproses pesan: {e}")

def on_error(ws, error):
    print(f"[ERROR] {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"[CLOSED] Koneksi ke {ws.url} terputus.")

def on_open(ws):
    print(f"[OPEN] Terhubung ke {ws.url}. Mengirim perintah 'start'...")
    ws.send("start")  # INI WAJIB AGAR SERVER MAU NGIRIM DATA

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
    url_penumpang = "ws://70.153.136.193:8765"
    url_gps = "ws://70.153.136.193:8766"

    print("Memulai Dual-Stream WebSocket ke Kafka...")
    
    # Menjalankan 2 WebSocket secara paralel dengan Threading
    t1 = threading.Thread(target=run_ws, args=(url_penumpang,))
    t2 = threading.Thread(target=run_ws, args=(url_gps,))
    
    t1.start()
    t2.start()
    
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\nDihentikan manual oleh user.")
    finally:
        producer.flush()