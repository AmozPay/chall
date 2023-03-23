import mgp
from typing import List, Dict
from math import log
logger = mgp.Logger()

def bellman_ford_traversal(
    vertices: mgp.Vertices,
    exchange: str,
    from_token: str,
) -> Dict[str, mgp.Edge]:
    logger.debug("Bellman start")
    distances: Dict[str, float] = {}
    previous_edges: Dict[str, mgp.Edge] = {}
    vertex: mgp.Vertex
    for vertex in vertices:
        distances[vertex.properties.get('name')] = float('inf')
    distances[from_token] = 0
    logger.debug(str(len(vertices)))
    for i in range(0, len(vertices) - 1):
        for vertex in vertices:
            edge: mgp.Edge
            for edge in vertex.out_edges:
                if edge.properties.get('exchange') != exchange:
                    continue
                neighbor_key = edge.to_vertex.properties.get('name')
                if neighbor_key in previous_edges: #avoid loop paths
                    continue
                current_key = vertex.properties.get('name')
                neighbor_distance = distances[neighbor_key]
                current_node_distance = distances[current_key]
                distance_in_between = log(edge.properties.get('opening_price'))
                if neighbor_distance > current_node_distance + distance_in_between:
                    distances[neighbor_key] = current_node_distance + distance_in_between
                    previous_edges[neighbor_key] = edge
    logger.debug("Bellman end")
    return previous_edges

def dump_edges(edges: Dict[str, mgp.Edge]):
    logger.debug("dumping")
    for key, value in edges.items():
        logger.debug(key + " " + value.from_vertex.properties.get('name'))
    logger.debug("end dumping")

def edges_to_array(traversed_edges: Dict[str, mgp.Edge], from_token: str, to_token: str) -> List[mgp.Edge]:
    reversed_path_array: List[mgp.Edge] = []
    current_edge_key = to_token
    logger.debug('reversing path')
    while current_edge_key != from_token:
        logger.debug(f"current edge key: {current_edge_key}")
        reversed_path_array.append(traversed_edges[current_edge_key])
        current_edge_key = traversed_edges[current_edge_key].from_vertex.properties.get('name')
    return reversed_path_array

def _find_best_trade_path(
    context: mgp.ProcCtx,
    exchange: str,
    from_token: str,
    to_token: str
) -> List[mgp.Edge]:
    logger.debug('start')
    traversed_edges = bellman_ford_traversal(context.graph.vertices, exchange, from_token)
    dump_edges(traversed_edges)
    reversed_path_array: List[mgp.Edge] = edges_to_array(traversed_edges, from_token, to_token)
    return reversed_path_array

def edges_array_to_path(start_vertex: mgp.Vertex, edges: List[mgp.Edge]) -> mgp.Path:
    path = mgp.Path(start_vertex)
    for i in range(1, len(edges) + 1):
        logger.debug(f"expanding path: {i}")
        path.expand(edges[-i])
        # rate *= edges[-i].properties.get('opening_price')
    return path

@mgp.read_proc
def find_best_trade_path(
    context: mgp.ProcCtx,
    exchange: str,
    from_token: str,
    to_token: str,
) -> mgp.Record(shortest_path=mgp.Nullable[mgp.Path]):
    reversed_path_array = _find_best_trade_path(context, exchange, from_token, to_token)
    logger.debug(reversed_path_array[-1].from_vertex.properties.get('name'))
    start_vertex: mgp.Vertex
    for vertex in context.graph.vertices:
        if vertex.properties.get('name') == from_token:
            start_vertex = vertex
    path = edges_array_to_path(start_vertex, reversed_path_array)
    logger.debug("first node:" + path.vertices[0].properties.get('name'))
    # path = mgp.Path(reversed_path_array[-1].from_vertex)

    return mgp.Record(shortest_path=path)

