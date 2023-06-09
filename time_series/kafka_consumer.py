from typing import List
# from confluent_kafka import Consumer, KafkaError, KafkaException
from kafka import KafkaConsumer
from setup import bootstrap
import json

from datetime import datetime


def create_tables(qdb):
    tokens = ["BTC", "USDT", "ETH", "ALGO"]
    with qdb.cursor() as cur:
        for token_a in tokens:
            for token_b in tokens:
                if token_a == token_b:
                    continue
                query = (
                    f"CREATE TABLE IF NOT EXISTS {token_a}_{token_b}"
                    + f"(ts TIMESTAMP, rate DOUBLE, exchange SYMBOL)"
                )
                cur.execute(query)

def msg_process(msg, qdb):
    value = json.loads(msg.value.decode('utf-8'))
    table = msg.key.decode('utf-8')
    exchange = value['exchange']
    rate = value['rate']
    timestamp = int(msg.timestamp / 1000)
    datetime_str = ":".join(str(datetime.fromtimestamp(timestamp)).split(' '))
    timestamp_func = f"to_timestamp('{datetime_str}', 'yyyy-MM-dd:HH:mm:ss')"
    with qdb.cursor() as cur:
        cur.execute(f"INSERT INTO {table} VALUES ({timestamp_func}, {rate}, '{exchange}')")

def consume_loop(consumer: KafkaConsumer, topics: List[str], qdb):
    try:
        consumer.subscribe(topics)
        for message in consumer:
            msg_process(message, qdb)
            consumer.commit()
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def main():
    qdb, kafka_consumer = bootstrap()
    print('Connection established')
    create_tables(qdb)
    print('Tables created')
    consume_loop(kafka_consumer, ['best_rates'], qdb)
    qdb.close()

if __name__ == "__main__":
    main()