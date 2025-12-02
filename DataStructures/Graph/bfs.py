import math as math
from DataStructures.Graph import digraph as G
from DataStructures.List import array_list as lt
from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as s
from DataStructures.Map import map_linear_probing as ml
def bfs(my_graph, source):
    visited_map = ml.new_map(num_elements=G.order(my_graph), load_factor=0.7, prime=109345121)
    ml.put(visited_map, source, {"edge_from": None,"dist_to": 0})
    bfs_vertex(my_graph, source, visited_map)
    return visited_map


def bfs_vertex(my_graph, source, visited_map):
    cola = q.new_queue()
    q.enqueue(cola, source)

    while not q.is_empty(cola):
        v = q.dequeue(cola)
        lista = G.adjacent(my_graph, v) #Es un mapa
        for i in range(lt.size(lista)):
            w = lt.get_element(lista, i)
            if w is not None:
                if G.contains_vertex(my_graph, w):
                    elem = ml.get(visited_map, w)
                    if elem is None:
                        info = ml.get(visited_map, v)
                        ml.put(visited_map, w, {"edge_from": v, "dist_to": info["dist_to"] + 1})
                        q.enqueue(cola, w)
                else:
                    continue
    return visited_map
        

def has_path_to(key_v, visited_map):
    return ml.contains(visited_map, key_v)

def path_to(key_v, visited_map):
    if not has_path_to(key_v, visited_map):
        return None
    else:
        stack=s.new_stack()
        current_key=key_v
        while current_key is not None:
            s.push(stack, current_key)
            info = ml.get(visited_map, current_key)
            current_key= info["edge_from"]
        return stack