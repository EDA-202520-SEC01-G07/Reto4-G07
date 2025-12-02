import math as math
from DataStructures.Graph import digraph as G
from DataStructures.List import array_list as lt
from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as s
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Map import map_linear_probing as ml
from DataStructures.Graph import dijsktra_structure as dj
#De la presentación
def init_structure(graph, source):
    djk = dj.new_dijsktra_structure(source, G.order(graph))
    vertices = G.vertices(graph)
    for i in range(lt.size(vertices)):
        elem = lt.get_element(vertices, i)
        ml.put(djk["visited"], elem, {"marked": False, "edge_from": None, "dist_to": math.inf})
    ml.put(djk["visited"], source, {"marked": False, "edge_from": None, "dist_to": 0})
    pq.insert(djk["pq"], 0.0, source)
    return djk
    
def dijkstra(graph, source):
    if not G.contains_vertex(graph,source):
        return None
    else:
        djk = init_structure(graph, source)
        pila = djk["pq"]
        marked = djk["visited"]
        
        while not pq.is_empty(pila):
            v_m = pq.remove(pila)
            marcado = ml.get(marked, v_m)
            if marcado is None:
                continue
            else:
                dist_vm = marcado["dist_to"]
                ml.put(marked, v_m, {"marked": True, "edge_from": marcado["edge_from"], "dist_to": dist_vm})
                adj = G.edges_vertex(graph, v_m)
                llaves = G.adjacent(graph, v_m)
                for i in range(lt.size(llaves)):
                    elem = lt.get_element(llaves, i)
                    peso = ml.get(adj, elem)["weight"]
                    mark = ml.get(marked, elem) #me da el value directamente
                    if mark is None:
                        continue
                    if mark["marked"] is False:
                        costo = peso + dist_vm
                        if costo < mark["dist_to"]:
                            mark["dist_to"] = costo
                            mark["edge_from"] = v_m
                            ml.put(marked, elem, {"marked": False, "edge_from": v_m, "dist_to": mark["dist_to"]})
                            pq.insert(pila, costo, elem)
        return djk
    
def dist_to(key_v, aux):
    nodo = ml.get(aux["visited"], key_v)
    return (nodo["dist_to"])
        
def has_path_to(key_v, aux):
    elem = ml.get(aux["visited"], key_v)
    if elem is not None and elem["dist_to"] != math.inf:
        return True
    return False

def path_to(key_v, aux):
    if not has_path_to(key_v, aux):
        return None
    else:
        stack = s.new_stack()
        while key_v != aux["source"]:
            s.push(stack, key_v)
            nodo = ml.get(aux["visited"], key_v)
            key_v = nodo["edge_from"]
        s.push(stack, aux["source"])
        return stack