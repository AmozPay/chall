import mgp
from typing import Dict

logger = mgp.Logger()
@mgp.read_proc
def buy_from_a_to_b(context: mgp.ProcCtx,
              token_a: str,
              token_b: str
              ) -> mgp.Record(exchange=mgp.Nullable[str], price=mgp.Nullable[float]):
    node_a: mgp.Vertex = None
    vertex: mgp.Vertex
    paths: Dict[str, float] = {}
    for vertex in context.graph.vertices:
        if token_a == vertex.properties.get('name'):
            node_a = vertex
    if node_a == None:
        return mgp.Record(exchange=None, price=None)
    edge: mgp.Edge
    for edge in [*node_a.in_edges, *node_a.out_edges]:
        if edge.from_vertex.properties.get('name') == token_b:
            paths[edge.properties.get('exchange')] = edge.properties.get('opening_price')
        if edge.to_vertex.properties.get('name') == token_b:
            paths[edge.properties.get('exchange')] = 1 / edge.properties.get('opening_price')

    if len(paths) == 0:
        return mgp.Record(exchange=None, price=None)
    cheapest_price: float = float('inf')
    cheapest_exchange: str
    for exchange, price in paths.items():
        if price < cheapest_price:
            cheapest_price = price
            cheapest_exchange = exchange
    return mgp.Record(exchange=cheapest_exchange, price=cheapest_price)
