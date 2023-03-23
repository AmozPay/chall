import json
from time import sleep
from typing import Dict, List
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import mgp

from find_best_path import _find_best_trade_path
from find_best_exchange import _buy_from_a_to_b

PRODUCER_SINGLETON: KafkaProducer = None
logger = mgp.Logger()

def create_kafka_producer(kafka_addr: str):
    global PRODUCER_SINGLETON
    retries = 30
    while True:
        try:
            PRODUCER_SINGLETON = KafkaProducer(
                bootstrap_servers=kafka_addr)
            return
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            print("Failed to connect to Kafka")
            sleep(1)

def producer(kafka_addr: str) -> KafkaProducer:
    global PRODUCER_SINGLETON
    if PRODUCER_SINGLETON == None:
        create_kafka_producer(kafka_addr)
    return PRODUCER_SINGLETON

@mgp.read_proc
def send_best_trade_paths(
    context: mgp.ProcCtx,
    kafka_addr: str
) -> mgp.Record():
    for exchange in ["okx", "binance", "kucoin", "huobi"]:
        vertex_a: mgp.Vertex
        vertex_b: mgp.Vertex
        best_paths_by_token_pair: Dict[str, List[Dict[str, str or float]]] = {}
        for vertex_a in context.graph.vertices:
            for vertex_b in context.graph.vertices:
                a_name = vertex_a.properties.get('name')
                b_name = vertex_b.properties.get('name')
                if a_name == b_name:
                    continue
                edges = _find_best_trade_path(
                    context,
                    exchange,
                    a_name,
                    b_name
                )
                edges.reverse()
                best_paths_by_token_pair[f"{a_name}/{b_name}"] = list(map(lambda edge: {
                    'from': edge.from_vertex.properties.get('name'),
                    'to': edge.to_vertex.properties.get('name'),
                    'opening_price': edge.properties.get('opening_price')
                }, edges))
        producer(kafka_addr).send(
            topic='best_routes',
            key=bytes(exchange, 'utf-8'),
            value=bytes(json.dumps(best_paths_by_token_pair), 'utf-8')
        )
    return mgp.Record()

@mgp.read_proc
def send_best_exchanges(
    context: mgp.ProcCtx,
    kafka_addr: str
) -> mgp.Record():
    for vertex_a in context.graph.vertices:
        for vertex_b in context.graph.vertices:
            a_name = vertex_a.properties.get('name')
            b_name = vertex_b.properties.get('name')
            if a_name == b_name:
                continue
            result = _buy_from_a_to_b(context, a_name, b_name)
            if result == None:
                continue
            cheapest_exchange, cheapest_price = result
            producer(kafka_addr).send(
                topic='best_rates',
                key=bytes(f"{a_name}_{b_name}", 'utf-8'),
                value=bytes(json.dumps({
                    'exchange': cheapest_exchange,
                    'rate': cheapest_price
                }), 'utf-8')
            )
    return mgp.Record()