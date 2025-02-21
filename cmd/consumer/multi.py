from kafka import KafkaConsumer, TopicPartition
from threading import Thread
import json

class AnalyticsConsumer(Thread):
    def __init__(self, bootstrap_servers, topic_name, consumer_id, group_id):
        Thread.__init__(self)
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.topic_name = topic_name
        self.consumer_id = consumer_id
        self.running = True

    def run(self):
        # Subscribe to topic
        self.consumer.subscribe([self.topic_name])
        
        try:
            while self.running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)
                
                for tp, records in messages.items():
                    for record in records:
                        # Process the message
                        print(f"\nConsumer {self.consumer_id} received message:")
                        print(f"Topic: {record.topic}")
                        print(f"Partition: {record.partition}")
                        print(f"Offset: {record.offset}")
                        print(f"Key: {record.key}")
                        print(f"Value: {record.value}")
                        
                        # Process different event types
                        event = record.value
                        if event['event_type'] == 'purchase':
                            self.process_purchase(event)
                        elif event['event_type'] == 'pageview':
                            self.process_pageview(event)
                        elif event['event_type'] == 'click':
                            self.process_click(event)

        except Exception as e:
            print(f"Error in consumer {self.consumer_id}: {e}")
        finally:
            self.consumer.close()

    def process_purchase(self, event):
        print(f"Processing purchase event for user {event['user_id']}")

    def process_pageview(self, event):
        print(f"Processing pageview event for user {event['user_id']}")

    def process_click(self, event):
        print(f"Processing click event for user {event['user_id']}")

    def stop(self):
        self.running = False

def main():
    BOOTSTRAP_SERVERS = ['localhost:9092']
    TOPIC_NAME = 'analytics-topic'
    GROUP_ID = 'analytics-consumer-group'
    NUM_CONSUMERS = 3

    # Create and start multiple consumer threads
    consumers = []
    for i in range(NUM_CONSUMERS):
        consumer = AnalyticsConsumer(BOOTSTRAP_SERVERS, TOPIC_NAME, f"consumer-{i}", GROUP_ID)
        consumers.append(consumer)
        consumer.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Shutting down consumers...")
        for consumer in consumers:
            consumer.stop()
            consumer.join()

if __name__ == "__main__":
    main()