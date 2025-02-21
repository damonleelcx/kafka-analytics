from kafka import KafkaConsumer, ConsumerRebalanceListener
from typing import Dict, Any, List
import json
import threading
import signal
import time
from datetime import datetime
from dataclasses import dataclass
from threading import Lock, Event

@dataclass
class Message:
    id: str
    timestamp: str
    value: float
    category: str
    partition: int

@dataclass
class Analytics:
    category: str
    partition: int
    count: int = 0
    sum: float = 0.0
    average: float = 0.0
    min: float = float('inf')
    max: float = float('-inf')
    last_updated: str = ""
    lock: Lock = Lock()

class AnalyticsStore:
    def __init__(self):
        self.store: Dict[str, Dict[int, Analytics]] = {}
        self.lock = Lock()
        
        # Initialize store for all categories and partitions
        categories = ["sales", "traffic", "users", "errors", "latency"]
        for category in categories:
            self.store[category] = {}
            for partition in range(5):
                self.store[category][partition] = Analytics(
                    category=category,
                    partition=partition
                )

    def update(self, msg: Message):
        with self.lock:
            if msg.category not in self.store:
                self.store[msg.category] = {}
            
            if msg.partition not in self.store[msg.category]:
                self.store[msg.category][msg.partition] = Analytics(
                    category=msg.category,
                    partition=msg.partition
                )
            
            stats = self.store[msg.category][msg.partition]
            with stats.lock:
                stats.count += 1
                stats.sum += msg.value
                stats.average = stats.sum / stats.count
                stats.min = min(stats.min, msg.value)
                stats.max = max(stats.max, msg.value)
                stats.last_updated = msg.timestamp

    def print_report(self):
        with self.lock:
            print("\n=== Analytics Report ===")
            for category, partition_stats in self.store.items():
                print(f"\nCategory: {category}")
                for partition, stats in partition_stats.items():
                    with stats.lock:
                        print(f"  Partition {partition}:")
                        print(f"    Count: {stats.count}")
                        print(f"    Average: {stats.average:.2f}")
                        print(f"    Min: {stats.min:.2f}")
                        print(f"    Max: {stats.max:.2f}")
                        print(f"    Last Updated: {stats.last_updated}")

class MessageProcessor:
    def __init__(self, analytics_store: AnalyticsStore):
        self.analytics_store = analytics_store
        self.workers = []
        self.message_queue: List[Message] = []
        self.queue_lock = Lock()
        self.stop_event = Event()

    def start_workers(self, num_workers: int):
        for i in range(num_workers):
            worker = threading.Thread(target=self._worker_loop, args=(i,))
            worker.start()
            self.workers.append(worker)

    def stop_workers(self):
        self.stop_event.set()
        for worker in self.workers:
            worker.join()

    def add_message(self, msg: Message):
        with self.queue_lock:
            self.message_queue.append(msg)

    def _worker_loop(self, worker_id: int):
        print(f"Worker {worker_id} started")
        while not self.stop_event.is_set():
            message = None
            with self.queue_lock:
                if self.message_queue:
                    message = self.message_queue.pop(0)
            
            if message:
                self.analytics_store.update(message)
            else:
                time.sleep(0.1)
        
        print(f"Worker {worker_id} stopped")

def main():
    # Initialize analytics store
    analytics_store = AnalyticsStore()
    
    # Initialize message processor
    processor = MessageProcessor(analytics_store)
    processor.start_workers(5)
    
    # Create consumer
    consumer = KafkaConsumer(
        'analytics-topic',
        bootstrap_servers=['localhost:9092'],
        group_id='analytics-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Setup shutdown handling
    stop_event = Event()
    def signal_handler(signum, frame):
        print("\nShutdown signal received...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start analytics reporter
    def report_analytics():
        while not stop_event.is_set():
            analytics_store.print_report()
            time.sleep(5)
    
    reporter_thread = threading.Thread(target=report_analytics)
    reporter_thread.start()
    
    # Main consumer loop
    try:
        while not stop_event.is_set():
            messages = consumer.poll(timeout_ms=100)
            for tp, msgs in messages.items():
                for msg in msgs:
                    message = Message(
                        id=msg.value['id'],
                        timestamp=msg.value['timestamp'],
                        value=msg.value['value'],
                        category=msg.value['category'],
                        partition=msg.partition
                    )
                    processor.add_message(message)
    
    finally:
        # Cleanup
        consumer.close()
        processor.stop_workers()
        stop_event.set()
        reporter_thread.join()

if __name__ == "__main__":
    main()