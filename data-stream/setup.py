import logging
from gqlalchemy import Memgraph, MemgraphKafkaStream, MemgraphTrigger
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from gqlalchemy.models import (
    TriggerEventType,
    TriggerEventObject,
    TriggerExecutionPhase,
)
from time import sleep


log = logging.getLogger(__name__)


def connect_to_memgraph(memgraph_ip, memgraph_port):
    memgraph = Memgraph(host=memgraph_ip, port=int(memgraph_port))
    while True:
        try:
            if memgraph._get_cached_connection().is_active():
                return memgraph
        except:
            log.info("Memgraph probably isn't running.")
            sleep(1)


def get_admin_client(kafka_ip, kafka_port):
    retries = 30
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=kafka_ip + ":" + kafka_port, client_id="data-stream"
            )
            return admin_client
        except NoBrokersAvailable:
            retries -= 1
            if not retries:
                raise
            log.info("Failed to connect to Kafka")
            sleep(1)


def run(memgraph, kafka_ip, kafka_port):
    admin_client = get_admin_client(kafka_ip, kafka_port)
    log.info("Connected to Kafka")

    topic_list = [
        NewTopic(name="prices", num_partitions=1, replication_factor=1),
        NewTopic(name="best_rates", num_partitions=1, replication_factor=1),
        NewTopic(name="best_routes", num_partitions=1, replication_factor=1),
    ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    log.info("Created topics")

    log.info("Creating stream connections on Memgraph")
    stream = MemgraphKafkaStream(
        name="prices_stream",
        topics=["prices"],
        transform="exchange.prices",
        bootstrap_servers="'kafka:9092'",
    )

    update_best_exchange = MemgraphTrigger(
        name="update_best_exchange",
        event_type=TriggerEventType.UPDATE,
        event_object=TriggerEventObject.RELATIONSHIP,
        execution_phase=TriggerExecutionPhase.AFTER,
        statement=f"CALL kafka_producer.send_best_exchanges('{kafka_ip}:{kafka_port}');",
    )

    update_best_path = MemgraphTrigger(
        name="update_best_path",
        event_type=TriggerEventType.UPDATE,
        event_object=TriggerEventObject.RELATIONSHIP,
        execution_phase=TriggerExecutionPhase.AFTER,
        statement=f"CALL kafka_producer.send_best_trade_paths('{kafka_ip}:{kafka_port}');",
    )
    memgraph.create_trigger(update_best_exchange)
    memgraph.create_trigger(update_best_path)
    memgraph.create_stream(stream)
    memgraph.start_stream(stream)
