import mgp
import json
from typing import Dict, Set, List

EXISTING_TOKEN_NODES: Set[str] = set()

class TradeRelation:
    _rate_parameters: Dict[str, float] = {}
    def __init__(self, exchange: str, token_a: str, token_b: str, rate_parameters: Dict[str, float]):
        self._token_a = token_a
        self._token_b = token_b
        self._exchange = exchange
        self._rate_parameters = rate_parameters


    def get_create_query(self) -> str:
        rates_array = map(lambda kv: f"r.{kv[0]} = {kv[1]}", self._rate_parameters.items())
        rates = ",".join(rates_array)

        create_query = f"MATCH (a:Token {{ name: '{self._token_a}' }}), (b:Token {{ name: '{self._token_b}' }})\n"
        create_query += f"MERGE (a) <-[r:SELLS_TO {{ exchange: '{self._exchange}', rate_direction: '{self._token_b}/{self._token_a}' }}]-> (b)\n"
        create_query += f"SET {rates}"
        create_query += f"RETURN a, r, b;"
        return create_query

def create_token_if_not_exists(token: str, result_queries: List[mgp.Record]) -> None:
    """
        This function replaces the MERGE query so that we don't spam the database while we could check before hand, reducing network requests
    """
    global EXISTING_TOKEN_NODES
    if not token in EXISTING_TOKEN_NODES:
        result_queries.append(mgp.Record(
            query=f"CREATE (a:Token {{ name: '{token}'}}) RETURN a",
            parameters=None
        ))
        EXISTING_TOKEN_NODES.add(token)

@mgp.transformation
def prices(messages: mgp.Messages
             ) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    result_queries = []
    logger = mgp.Logger()
    for i in range(messages.total_messages()):
        message = messages.message_at(i)
        payload_as_str = message.payload().decode("utf-8")
        payload_as_dict = json.loads(payload_as_str)
        token_pair = message.key().decode('utf-8')
        logger.debug(token_pair)
        token_a, token_b = token_pair.split('_')
        for exchange, prices in payload_as_dict.items():
            create_token_if_not_exists(token_a, result_queries)
            create_token_if_not_exists(token_b, result_queries)
            opening_price, highest_price, lowest_price, closing_price, transaction_amount = prices
            relation = TradeRelation(exchange, token_a, token_b, {
                'opening_price': opening_price,
                'highest_price': highest_price,
                'lowest_price': lowest_price,
                'closing_price': closing_price,
                'transaction_amount': transaction_amount,
                'timestamp': message.timestamp()
            })
            result_queries.append(mgp.Record(
                query=relation.get_create_query(),
                parameters=None)
            )


    return result_queries
