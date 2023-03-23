from gqlalchemy import (
    Memgraph,
    MemgraphTrigger,
)
from gqlalchemy.models import (
    TriggerEventType,
    TriggerEventObject,
    TriggerExecutionPhase,
)
from time import sleep
import logging
from argparse import ArgumentParser

log = logging.getLogger("server")

def connect_to_memgraph(memgraph_addr: str):
    host, port = memgraph_addr.split(':')
    memgraph = Memgraph(host=host, port=int(port))
    while True:
        try:
            if memgraph._get_cached_connection().is_active():
                return memgraph
        except:
            log.info("Memgraph probably isn't running.")
            sleep(1)


def run(memgraph: Memgraph, kafka_addr: str):
    try:

        log.info("Setting up community detection")

        log.info("Creating stream connections on Memgraph")

        log.info("Creating triggers on Memgraph")
        update_best_exchange = MemgraphTrigger(
            name="update_best_exchange",
            event_type=TriggerEventType.UPDATE,
            event_object=TriggerEventObject.RELATIONSHIP,
            execution_phase=TriggerExecutionPhase.AFTER,
            statement=f"CALL kafka_producer.send_best_exchanges('{kafka_addr}');",
        )

        update_best_path = MemgraphTrigger(
            name="update_best_path",
            event_type=TriggerEventType.UPDATE,
            event_object=TriggerEventObject.RELATIONSHIP,
            execution_phase=TriggerExecutionPhase.AFTER,
            statement=f"CALL kafka_producer.send_best_trade_paths('{kafka_addr}');",
        )
        memgraph.create_trigger(update_best_exchange)
        memgraph.create_trigger(update_best_path)


    except Exception as e:
        log.info(f"Error on stream and trigger creation: {e}")
        pass

def parse_args():
    """
    Parse input command line arguments.
    """
    parser = ArgumentParser(description="Create a trigger on price update in kafka")
    parser.add_argument("--kafka-addr", help="Ip and port of the kafka instance, using schema IP:PORT")
    parser.add_argument(
        "--memgraph-addr",
        help="Ip and port of the memgraph instance, using schema IP:PORT")
    return parser.parse_args()


def main():
    args = parse_args()
    memgraph = connect_to_memgraph(args.memgraph_addr)
    run(memgraph, args.kafka_addr)

if __name__ == "__main__":
    main()