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
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic_name = topic_name
        self.producer_id = producer_id
        self.running = True

    def run(self):
        counter = 0
        while self.running:
            try:
                # Generate sample analytics data
                event_types = ['pageview', 'click', 'purchase']
                event = {
                    'timestamp': datetime.now().isoformat(),
                    'event_type': random.choice(event_types),
                    'user_id': random.randint(1, 1000),
                    'producer_id': self.producer_id,
                    'message_number': counter
                }
                
                # Debug print
                print(f"Producer {self.producer_id} sending message {counter}:", event)
                
                key = str(event['user_id']).encode('utf-8')
                
                future = self.producer.send(
                    self.topic_name,
                    key=key,
                    value=event
                )
                
                record_metadata = future.get(timeout=10)
                print(f"Successfully sent message {counter} to topic {record_metadata.topic} "
                      f"partition {record_metadata.partition} offset {record_metadata.offset}")
                
                counter += 1
                time.sleep(1)  # Increased delay for better visibility
                
            except Exception as e:
                print(f"Error in producer {self.producer_id}: {str(e)}")
                time.sleep(1)

    def stop(self):
        self.running = False
        self.producer.close()

def main():
    BOOTSTRAP_SERVERS = ['localhost:9092']
    TOPIC_NAME = 'analytics-topic'
    NUM_PRODUCERS = 1  # Reduced to 1 for testing

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