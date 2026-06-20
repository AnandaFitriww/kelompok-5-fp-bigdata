import json
import websocket
from kafka import KafkaProducer

# 1. Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'topic-transjakarta-ws'

WS_URL = "ws://70.153.136.193:8765" 

def on_message(ws, message):
    try:
        # Asumsi data yang masuk dari WebSocket berbentuk JSON string
        data = json.loads(message)
        
        # Kirim data mentah langsung ke broker Kafka
        producer.send(TOPIC_NAME, data)
        print(f"[WEBSOCKET -> KAFKA] Data berhasil diteruskan ke Kafka: {data}")
        
    except Exception as e:
        print(f"Gagal memproses pesan: {e}")

def on_error(ws, error):
    print(f"=== WEBSOCKET ERROR ===\n{error}")

def on_close(ws, close_status_code, close_msg):
    print("=== KONEKSI WEBSOCKET TERPUTUS ===")

def on_open(ws):
    print("=== KONEKSI WEBSOCKET BERHASIL DIBUKA ===\nStreaming data dimulai...")

if __name__ == "__main__":
    # Menghubungkan ke server WebSocket live
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nKoneksi dihentikan oleh user.")
    finally:
        producer.flush()