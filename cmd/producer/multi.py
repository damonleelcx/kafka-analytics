from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.partitioner.default import DefaultPartitioner  # Updated import
import json
import random
import time
import signal
import threading
from datetime import datetime
from typing import Dict, Any

class Message:
    def __init__(self, id: str, category: str, value: float):
        self.id = id
        self.timestamp = datetime.now().isoformat()
        self.value = value
        self.category = category

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "value": self.value,
            "category": self.category
        }

class CustomPartitioner(DefaultPartitioner):  # Inherit from DefaultPartitioner
    def __init__(self, partitions):
        super().__init__(partitions)
        self.category_partition_map = {
            "sales": 0,
            "traffic": 1,
            "users": 2,
            "errors": 3,
            "latency": 4
        }

    def __call__(self, key_bytes, all_partitions, available_partitions):
        if key_bytes is None:
            return super().__call__(key_bytes, all_partitions, available_partitions)
        
        category = key_bytes.decode('utf-8')
        return self.category_partition_map.get(category, super().__call__(key_bytes, all_partitions, available_partitions))

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: v.encode('utf-8'),
        partitioner=CustomPartitioner  # Use the custom partitioner
    )

def ensure_topic_exists(topic_name: str, num_partitions: int):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        if topic_name not in admin_client.list_topics():
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=1
            )
            admin_client.create_topics([topic])
            print(f"Created topic {topic_name} with {num_partitions} partitions")
        admin_client.close()
    except Exception as e:
        print(f"Error ensuring topic exists: {e}")

def produce_messages(producer: KafkaProducer, category: str, stop_event: threading.Event):
    message_count = 0
    
    while not stop_event.is_set():
        try:
            msg = Message(
                id=f"msg-{category}-{message_count}",
                category=category,
                value=random.random() * 100
            )

            future = producer.send(
                'analytics-topic',
                key=category,
                value=msg.to_dict()
            )
            
            if message_count % 100 == 0:
                metadata = future.get(timeout=1)
                print(f"Producer (Category: {category}): Message sent! "
                      f"Partition: {metadata.partition}, Offset: {metadata.offset}")
            
            message_count += 1
            time.sleep(0.1)  # 100ms delay between messages
            
        except Exception as e:
            print(f"Error producing message: {e}")
            
    print(f"Producer for category {category} shutting down...")

def main():
    # Ensure topic exists with correct number of partitions
    ensure_topic_exists('analytics-topic', 5)
    
    # Create producer
    producer = create_producer()
    
    # Setup shutdown handling
    stop_event = threading.Event()
    def signal_handler(signum, frame):
        print("\nShutdown signal received, closing producers...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start producer threads
    categories = ["sales", "traffic", "users", "errors", "latency"]
    threads = []
    
    for category in categories:
        thread = threading.Thread(
            target=produce_messages,
            args=(producer, category, stop_event)
        )
        thread.start()
        threads.append(thread)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Clean up
    producer.close()

if __name__ == "__main__":
    main()