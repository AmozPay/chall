import psycopg2
import os
from time import sleep
from kafka import KafkaConsumer

QDB_HOST = os.environ.get('QDB_HOST')
QDB_PORT = os.environ.get('QDB_PORT')
QDB_PG_USER = os.environ.get('QDB_PG_USER')
QDB_PG_PASSWORD = os.environ.get('QDB_PG_PASSWORD')

KAFKA_IP = os.environ.get('KAFKA_IP')
KAFKA_PORT = os.environ.get('KAFKA_PORT')

def create_qdb_connection():
    retries = 30
    while True:
        try:
            qdb_connection = psycopg2.connect(
                user=QDB_PG_USER,
                password=QDB_PG_PASSWORD,
                host=QDB_HOST,
                port=QDB_PORT,
                database="qdb",
            )
            qdb_connection.autocommit = True
            return qdb_connection
        except:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Quest DB")
            sleep(1)

def create_kafka_consumer():
    retries = 30
    while True:
        try:
            consumer = KafkaConsumer(bootstrap_servers=KAFKA_IP + ':' + KAFKA_PORT, group_id='questdb')
            return consumer
        except :
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)

def bootstrap():
    return create_qdb_connection(), create_kafka_consumer()
