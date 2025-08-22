import json
import time
import os

from kafka import KafkaProducer

from event_generator import SeismicEventGenerator

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

# Producer configuration
PRODUCE_INTERVAL_SECS = float(os.getenv('PRODUCE_INTERVAL_SECS'))

class SeismicEventProducer:

    def __init__(self):
        self.event_generator = SeismicEventGenerator()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )

    def produce_event(self, event):
        self.producer.send(
            topic=KAFKA_TOPIC,
            key=event.get('id'),
            value=event
        )

    def run(self):
        while True:
            start_time = time.time()

            event = self.event_generator.generate_event()
            self.produce_event(event)

            elapsed = time.time() - start_time
            sleep_time = max(0, PRODUCE_INTERVAL_SECS - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

def main():
    producer = SeismicEventProducer()
    producer.run()

if __name__ == '__main__':
    main()
