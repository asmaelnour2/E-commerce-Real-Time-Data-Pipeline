import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'clickstream'
EVENT_TYPES = ['user_login', 'product_view', 'add_to_cart', 'purchase']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']

def generate_clickstream_event():
    user_id = f"USR_{random.randint(1000, 9999)}"
    event_type = random.choices(EVENT_TYPES, weights=[0.4, 0.3, 0.2, 0.1], k=1)[0]
    
    event = {
        "event_id": f"EVT_{random.randint(100000, 999999)}",
        "user_id": user_id,
        "event_type": event_type,
        "device": random.choice(DEVICE_TYPES),
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "metadata": {
            "product_id": f"PROD_{random.randint(100, 500)}" if event_type in ['product_view', 'add_to_cart', 'purchase'] else None,
            "amount": round(random.uniform(10.0, 500.0), 2) if event_type == 'purchase' else 0.0
        }
    }
    return event

if __name__ == "__main__":
    print(f"Starting Real-Time Kafka Producer on topic: {TOPIC_NAME}...")
    try:
        while True:
            payload = generate_clickstream_event()
            print(f"Sending data: {payload}")
            
        
            producer.send(TOPIC_NAME, value=payload)
            
            
            time.sleep(random.uniform(0.5, 1.5))
            
    except KeyboardInterrupt:
        print("Stopping Kafka Producer...")
    finally:
        producer.flush()
        producer.close()