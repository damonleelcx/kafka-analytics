from kafka import KafkaProducer
from threading import Thread
import json
import time
import random
from datetime import datetime

class AnalyticsProducer(Thread):
    def __init__(self, bootstrap_servers, topic_name, producer_id):
        Thread.__init__(self)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic_name = topic_name
        self.producer_id = producer_id
        self.running = True

    def run(self):
        while self.running:
            # Generate sample analytics data
            event_types = ['pageview', 'click', 'purchase']
            event = {
                'timestamp': datetime.now().isoformat(),
                'event_type': random.choice(event_types),
                'user_id': random.randint(1, 1000),
                'producer_id': self.producer_id
            }
            
            # Use user_id as key for consistent partition routing
            key = str(event['user_id'])
            
            # Send message
            future = self.producer.send(
                self.topic_name,
                key=key,
                value=event
            )
            
            try:
                record_metadata = future.get(timeout=10)
                print(f"Producer {self.producer_id} sent event to topic {record_metadata.topic} "
                      f"partition {record_metadata.partition} offset {record_metadata.offset}")
            except Exception as e:
                print(f"Error sending message: {e}")
            
            time.sleep(random.uniform(0.1, 2.0))  # Random delay between messages

    def stop(self):
        self.running = False

def main():
    BOOTSTRAP_SERVERS = ['localhost:9092']
    TOPIC_NAME = 'analytics-topic'
    NUM_PRODUCERS = 3

    # Create and start multiple producer threads
    producers = []
    for i in range(NUM_PRODUCERS):
        producer = AnalyticsProducer(BOOTSTRAP_SERVERS, TOPIC_NAME, f"producer-{i}")
        producers.append(producer)
        producer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down producers...")
        for producer in producers:
            producer.stop()
            producer.join()

if __name__ == "__main__":
    main()