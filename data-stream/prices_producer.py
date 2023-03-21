import json
import os
from argparse import ArgumentParser
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from time import sleep
from data_ingest_mockup import ExchangePriceIngest
import setup


KAFKA_IP = os.getenv('KAFKA_IP', 'kafka')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
MEMGRAPH_IP = os.getenv('MEMGRAPH_IP', 'memgraph-mage')
MEMGRAPH_PORT = os.getenv('MEMGRAPH_PORT', '7687')


def parse_args():
    """
    Parse input command line arguments.
    """
    parser = ArgumentParser(
        description="An exchange stream machine.")
    parser.add_argument("--data-directory", help="Directory with prices data.")
    parser.add_argument(
        "--interval",
        type=int,
        help="Interval for sending data in seconds.")
    return parser.parse_args()


def create_kafka_producer():
    retries = 30
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_IP + ':' + KAFKA_PORT)
            return producer
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)

def main():
    args = parse_args()

    memgraph = setup.connect_to_memgraph(MEMGRAPH_IP, MEMGRAPH_PORT)
    setup.run(memgraph, KAFKA_IP, KAFKA_PORT)

    producer = create_kafka_producer()

    # TODO push data to the Kafka topic

    price_ingest = ExchangePriceIngest(args.data_directory)
    while True:
        for token_pair in price_ingest.token_pairs:
            timestamp, current_price = price_ingest.get_current_price(token_pair)
            print(token_pair, timestamp)
            print(current_price)
            producer.send(
                topic="prices",
                key=bytes(token_pair, 'utf-8'),
                value=bytes(json.dumps(current_price), 'utf-8'),
                timestamp_ms=timestamp
            )
        sleep(args.interval)

if __name__ == "__main__":
    main()
