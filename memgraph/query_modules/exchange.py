import mgp
import json


@mgp.transformation
def prices(messages: mgp.Messages
             ) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []

    # TODO implement your transformation module for Kafka

    return result_queries
