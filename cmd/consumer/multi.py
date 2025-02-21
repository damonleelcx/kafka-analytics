from kafka import KafkaConsumer
from threading import Thread
import json
import time

class AnalyticsConsumer(Thread):
    def __init__(self, bootstrap_servers, topic_name, consumer_id, group_id):
        Thread.__init__(self)
        self.consumer = KafkaConsumer(
            topic_name,  # Subscribe to topic during initialization
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.topic_name = topic_name
        self.consumer_id = consumer_id
        self.running = True

    def run(self):
        try:
            print(f"Consumer {self.consumer_id} started and waiting for messages...")
            
            while self.running:
                # Use message iterator instead of poll()
                for record in self.consumer:
                    try:
                        # Debug print raw message
                        print(f"\nConsumer {self.consumer_id} received raw message:")
                        print(f"Topic: {record.topic}")
                        print(f"Partition: {record.partition}")
                        print(f"Offset: {record.offset}")
                        print(f"Key: {record.key.decode('utf-8') if record.key else None}")
                        print(f"Raw Value: {record.value}")  # Print raw value for debugging
                        
                        # Validate message structure
                        if not isinstance(record.value, dict):
                            print(f"Warning: Received non-dict message: {record.value}")
                            continue
                            
                        if 'event_type' not in record.value:
                            print(f"Warning: Message missing event_type: {record.value}")
                            continue
                        
                        # Process the message based on event type
                        event_type = record.value.get('event_type')
                        user_id = record.value.get('user_id')
                        
                        if event_type == 'purchase':
                            self.process_purchase(record.value)
                        elif event_type == 'pageview':
                            self.process_pageview(record.value)
                        elif event_type == 'click':
                            self.process_click(record.value)
                        else:
                            print(f"Unknown event type: {event_type}")
                            
                    except Exception as e:
                        print(f"Error processing message in consumer {self.consumer_id}: {str(e)}")
                        print(f"Problematic message: {record.value}")
                        continue  # Continue with next message despite error

        except Exception as e:
            print(f"Critical error in consumer {self.consumer_id}: {str(e)}")
        finally:
            self.consumer.close()
            print(f"Consumer {self.consumer_id} shut down successfully")

    def process_purchase(self, event):
        print(f"Processing purchase event for user {event.get('user_id')}")

    def process_pageview(self, event):
        print(f"Processing pageview event for user {event.get('user_id')}")

    def process_click(self, event):
        print(f"Processing click event for user {event.get('user_id')}")

    def stop(self):
        self.running = False

def main():
    BOOTSTRAP_SERVERS = ['localhost:9092']
    TOPIC_NAME = 'analytics-topic'
    GROUP_ID = 'analytics-consumer-group'
    NUM_CONSUMERS = 1  # Reduced to 1 for testing

    consumers = []
    for i in range(NUM_CONSUMERS):
        consumer = AnalyticsConsumer(BOOTSTRAP_SERVERS, TOPIC_NAME, f"consumer-{i}", GROUP_ID)
        consumers.append(consumer)
        consumer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down consumers...")
        for consumer in consumers:
            consumer.stop()
            consumer.join()

if __name__ == "__main__":
    main()