import mgp
from typing import List, Dict
from math import log

logger = mgp.Logger()

def bellman_ford_pathfinding(
    vertices: mgp.Vertices,
    exchange: str,
    from_token: str
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
                current_key = vertex.properties.get('name')
                neighbor_distance = distances[neighbor_key]
                current_node_distance = distances[current_key]
                distance_in_between = log(edge.properties.get('opening_price'))
                if neighbor_distance > current_node_distance + distance_in_between:
                    if neighbor_key in previous_edges: #avoid loops
                        continue
                    distances[neighbor_key] = current_node_distance + distance_in_between
                    previous_edges[neighbor_key] = edge
    logger.debug("Bellman end")
    return previous_edges

def dump_edges(edges: Dict[str, mgp.Edge]):
    logger.debug("dumping")
    for key, value in edges.items():
        logger.debug(key + " " + value.from_vertex.properties.get('name'))
    logger.debug("end dumping")

@mgp.read_proc
def find_best_trade_path(
    context: mgp.ProcCtx,
    exchange: str,
    from_token: str,
    to_token: str
) -> mgp.Record(shortest_path=mgp.Nullable[mgp.Path], total_rate=mgp.Nullable[float]):
    logger.debug('start')
    reversed_path_array: List[mgp.Edge] = []
    previous_edges = bellman_ford_pathfinding(context.graph.vertices, exchange, from_token)
    dump_edges(previous_edges)
    current_edge_key = to_token
    logger.debug('reversing path')
    while current_edge_key != from_token:
        logger.debug(f"current edge key: {current_edge_key}")
        reversed_path_array.append(previous_edges[current_edge_key])
        current_edge_key = previous_edges[current_edge_key].from_vertex.properties.get('name')
    logger.debug('reversed path')
    # reversed_path_array.append(previous_edges[from_token])
    logger.debug(reversed_path_array[-1].from_vertex.properties.get('name'))
    path: mgp.Path
    for vertex in context.graph.vertices:
        if vertex.properties.get('name') == from_token:
            path = mgp.Path(vertex)
    logger.debug("first node:" + path.vertices[0].properties.get('name'))
    # path = mgp.Path(reversed_path_array[-1].from_vertex)
    rate = 1
    for i in range(1, len(reversed_path_array) + 1):
        logger.debug(f"expanding path: {i}")
        path.expand(reversed_path_array[-i])
        rate *= reversed_path_array[-i].properties.get('opening_price')
    logger.debug(f"len(path.edges) = {len(path.edges)}")
    logger.debug('end')
    return mgp.Record(shortest_path=path, total_rate=rate)
