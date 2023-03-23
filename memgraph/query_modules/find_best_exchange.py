import mgp
from typing import Dict, Tuple, List
logger = mgp.Logger()

def _buy_from_a_to_b(
    context: mgp.ProcCtx,
    token_a: str,
    token_b: str
) -> mgp.Edge or None:
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
    edges: List[mgp.Edge] = []
    for edge in node_a.out_edges:
        if edge.to_vertex.properties.get('name') == token_b:
            edges.append(edge)
            paths[edge.properties.get('exchange')] = edge.properties.get('opening_price')
    if len(edges) == 0:
        return None
    cheapest_edge: mgp.Edge = None
    for edge in edges:
        if cheapest_edge == None or (
            cheapest_edge.properties.get('opening_price')
            > edge.properties.get('opening_price')
        ):
            cheapest_edge = edge
    return cheapest_edge

@mgp.read_proc
def buy_from_a_to_b(
    context: mgp.ProcCtx,
    token_a: str,
    token_b: str
) -> mgp.Record(exchange=mgp.Nullable[str], price=mgp.Nullable[float]):
    result = _buy_from_a_to_b(context, token_a, token_b)
    if result == None:
        return mgp.Record(exchange=None, price=None)
    return mgp.Record(exchange=result.properties.get('exchange'), price=result.properties.get('opening_price'))
