"""
kafka_engine.py
Resilient Dual-Mode Kafka Engine.
Supports:
1. Native Apache Kafka cluster via kafka-python-ng (when broker at localhost:9092 is active)
2. High-performance Local Streaming Broker with WAL and persistent consumer offsets for zero-setup execution.
"""

import os
import json
import time
import sys
import threading
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# Try importing real kafka library
try:
    from kafka import KafkaProducer as RealKafkaProducer
    from kafka import KafkaConsumer as RealKafkaConsumer
    from kafka.errors import NoBrokersAvailable
    HAS_KAFKA_LIB = True
except ImportError:
    HAS_KAFKA_LIB = False
    NoBrokersAvailable = Exception

_probe_cache = {'t': 0.0, 'ok': False}


def kafka_reachable(bootstrap_servers=None, ttl=30, timeout=1.0):
    """Cheap TCP probe (cached) so status polling never blocks on a dead broker."""
    import socket
    now = time.time()
    if now - _probe_cache['t'] < ttl:
        return _probe_cache['ok']
    target = (bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS).split(',')[0]
    host, _, port = target.partition(':')
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=timeout):
            ok = True
    except Exception:
        ok = False
    _probe_cache.update(t=now, ok=ok)
    return ok


class LocalStreamingBroker:
    """Thread-safe, partitioned, disk-backed streaming broker emulator."""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(LocalStreamingBroker, cls).__new__(cls)
                cls._instance.topics = defaultdict(list)
                cls._instance.offsets = defaultdict(int)
                cls._instance.listeners = defaultdict(list)
                cls._instance.topic_locks = defaultdict(threading.Lock)
                cls._instance.wal_file = config.WAL_FILE
                cls._instance.offset_file = config.OFFSET_FILE
                cls._instance._load_state()
        return cls._instance

    def _load_state(self):
        # Load offsets
        if os.path.exists(self.offset_file):
            try:
                with open(self.offset_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.offsets = defaultdict(int, data)
            except Exception:
                pass
                
        # Load WAL into topics
        if os.path.exists(self.wal_file):
            try:
                with open(self.wal_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            entry = json.loads(line)
                            topic = entry['topic']
                            self.topics[topic].append(entry['record'])
            except Exception:
                pass

    def _save_offsets(self):
        try:
            with open(self.offset_file, 'w', encoding='utf-8') as f:
                json.dump(dict(self.offsets), f, indent=2)
        except Exception:
            pass

    def publish(self, topic, key, value):
        with self.topic_locks[topic]:
            offset = len(self.topics[topic])
            record = {
                'offset': offset,
                'timestamp': time.time(),
                'key': key,
                'value': value
            }
            self.topics[topic].append(record)
            
            # Persist to WAL
            try:
                with open(self.wal_file, 'a', encoding='utf-8') as f:
                    f.write(json.dumps({'topic': topic, 'record': record}) + '\n')
            except Exception:
                pass
            
            for listener in list(self.listeners[topic]):
                try:
                    listener(record)
                except Exception:
                    pass
            return offset

    def fetch(self, topic, group_id, max_records=500):
        group_key = f"{topic}:{group_id}"
        with self.topic_locks[topic]:
            # Always ensure we have latest records from WAL
            if not self.topics[topic] and os.path.exists(self.wal_file):
                self._load_state()
                
            cur_offset = self.offsets[group_key]
            records = self.topics[topic][cur_offset:cur_offset + max_records]
            self.offsets[group_key] = cur_offset + len(records)
            self._save_offsets()
            return records

    def get_topic_stats(self, topic):
        with self.topic_locks[topic]:
            return {
                'topic': topic,
                'total_messages': len(self.topics[topic]),
                'last_message_time': self.topics[topic][-1]['timestamp'] if self.topics[topic] else None
            }

    def reset_topic(self, topic):
        with self.topic_locks[topic]:
            self.topics[topic] = []
            if os.path.exists(self.wal_file):
                try:
                    os.remove(self.wal_file)
                except Exception:
                    pass
            if os.path.exists(self.offset_file):
                try:
                    os.remove(self.offset_file)
                except Exception:
                    pass
            self.offsets.clear()


class ResilientKafkaProducer:
    def __init__(self, bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.is_real_kafka = False
        self.producer = None
        self.local_broker = LocalStreamingBroker()
        
        if HAS_KAFKA_LIB:
            try:
                self.producer = RealKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                    request_timeout_ms=10000,
                    max_block_ms=8000,
                    acks='all'
                )
                self.is_real_kafka = True
                print(f"[Kafka Producer] Connected to real Apache Kafka broker at {self.bootstrap_servers}")
            except Exception as e:
                print(f"[Kafka Producer] Apache Kafka cluster not detected ({e}). Using Local Streaming Engine.")
                self.producer = None
                self.is_real_kafka = False
        else:
            print("[Kafka Producer] kafka-python not detected. Using Local Streaming Engine.")

    def send(self, topic, value, key=None):
        if self.is_real_kafka and self.producer:
            try:
                future = self.producer.send(topic, key=key, value=value)
                return future
            except Exception as e:
                self.is_real_kafka = False
                return self.local_broker.publish(topic, key, value)
        else:
            return self.local_broker.publish(topic, key, value)

    def flush(self):
        if self.is_real_kafka and self.producer:
            try:
                self.producer.flush()
            except Exception:
                pass

    def close(self):
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close(timeout=5)
            except Exception:
                pass

    def get_status(self):
        return {
            'mode': 'Apache Kafka (Cluster)' if self.is_real_kafka else 'Local Streaming Engine (Resilient Fallback)',
            'bootstrap_servers': self.bootstrap_servers if self.is_real_kafka else 'Local Buffer',
            'is_real_kafka': self.is_real_kafka
        }


class ResilientKafkaConsumer:
    def __init__(self, topic, group_id='hdfs-ingest-group', bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.is_real_kafka = False
        self.consumer = None
        self.local_broker = LocalStreamingBroker()

        if HAS_KAFKA_LIB:
            try:
                self.consumer = RealKafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=1000,
                    session_timeout_ms=10000,
                    request_timeout_ms=30000
                )
                self.is_real_kafka = True
                print(f"[Kafka Consumer] Connected to real Apache Kafka broker at {self.bootstrap_servers}")
            except Exception as e:
                print(f"[Kafka Consumer] Apache Kafka cluster not detected ({e}). Using Local Streaming Engine.")
                self.consumer = None
                self.is_real_kafka = False
        else:
            print("[Kafka Consumer] Using Local Streaming Engine.")

    def close(self):
        """Close the real consumer (commits offsets so messages are not re-read)."""
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass

    def poll(self, max_records=500):
        """Poll messages from either real Kafka or local streaming engine."""
        messages = []
        if self.is_real_kafka and self.consumer:
            try:
                raw_msgs = self.consumer.poll(timeout_ms=1000, max_records=max_records)
                for tp, batch in raw_msgs.items():
                    for msg in batch:
                        messages.append({
                            'offset': msg.offset,
                            'key': msg.key.decode('utf-8') if msg.key else None,
                            'value': msg.value
                        })
                return messages
            except Exception:
                self.is_real_kafka = False
        
        # Local broker poll
        records = self.local_broker.fetch(self.topic, self.group_id, max_records=max_records)
        return records
