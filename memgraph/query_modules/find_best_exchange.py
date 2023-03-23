import mgp
from typing import Dict, Tuple
logger = mgp.Logger()

def _buy_from_a_to_b(
    context: mgp.ProcCtx,
    token_a: str,
    token_b: str
) -> Tuple[str, float] or None:
    node_a: mgp.Vertex = None
    vertex: mgp.Vertex
    paths: Dict[str, float] = {}
    if len(context.graph.vertices) == 0:
        return None
    for vertex in context.graph.vertices:
        if token_a == vertex.properties.get('name'):
            node_a = vertex
    if node_a == None:
        return None
    edge: mgp.Edge
    for edge in node_a.out_edges:
        if edge.to_vertex.properties.get('name') == token_b:
            paths[edge.properties.get('exchange')] = edge.properties.get('opening_price')

    if len(paths.items()) == 0:
        return None
    cheapest_price: float = float('inf')
    cheapest_exchange: str
    for exchange, price in paths.items():
        if price < cheapest_price:
            cheapest_price = price
            cheapest_exchange = exchange
    return (cheapest_exchange, cheapest_price)

@mgp.read_proc
def buy_from_a_to_b(
    context: mgp.ProcCtx,
    token_a: str,
    token_b: str
) -> mgp.Record(exchange=mgp.Nullable[str], price=mgp.Nullable[float]):
    result = _buy_from_a_to_b(context, token_a, token_b)
    if result == None:
        return mgp.Record(exchange=None, price=None)
    cheapest_exchange, cheapest_price = result
    return mgp.Record(exchange=cheapest_exchange, price=cheapest_price)
