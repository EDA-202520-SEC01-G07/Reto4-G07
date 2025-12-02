from DataStructures.List import array_list as lt
from DataStructures.Map import map_linear_probing as ml
from DataStructures.Graph import vertex as vert
def new_graph(order):
    graph= {
        "vertices" :ml.new_map(order, 0.5, prime=109345121), 
        "num_edges":0
    }
    return graph
    
def insert_vertex(my_graph, key_u, info_u):
    vertex = vert.new_vertex(key_u, info_u)
    ml.put(my_graph["vertices"], key_u, vertex)
    return my_graph
    

def add_edge (my_graph, key_u, key_v, weight=1.0):
    vertex_u = ml.get(my_graph["vertices"], key_u)
    vertex_v = ml.get(my_graph["vertices"], key_v)
    if vertex_u is None:
        raise Exception("El vertice u no existe")
    if vertex_v is None:
        raise Exception("El vertice v no existe")
    
    vertex_u = vert.add_adjacent(vertex_u, key_v, weight)
    my_graph["num_edges"] += 1
    return my_graph

def contains_vertex(my_graph, key_u):
    return ml.contains(my_graph["vertices"], key_u)

def order(my_graph):
    return ml.size(my_graph["vertices"])

def size(my_graph):
    return my_graph["num_edges"]

def degree(my_graph, key_u):
    vertex_u = ml.get(my_graph["vertices"], key_u)
    if vertex_u is None:
        raise Exception("El vertice u no existe")
    return ml.size(vertex_u["adjacents"])

def adjacent(my_graph, key_u):  #Retorna key_set, no un mapa
    vertex_u = ml.get(my_graph["vertices"], key_u)
    if vertex_u is not None:
        lista = ml.key_set(vertex_u["adjacents"])
        return lista
    return Exception("El vertice u no existe")

def vertices (my_graph):
    keys = ml.key_set(my_graph["vertices"])
    return keys

def edges_vertex(my_graph, key_u):
    vertex_u = ml.get(my_graph["vertices"], key_u)
    if vertex_u is None:
        raise Exception("El vertice u no existe")
    return vertex_u["adjacents"]

def get_vertex(my_graph, key_u):
    vertex_u = ml.get(my_graph["vertices"], key_u)
    if vertex_u is None:
        raise Exception("El vertice u no existe")
    return vertex_u

def update_vertex_info(my_graph, key_u, new_info):
    vertex_u = ml.get(my_graph["vertices"], key_u)
    if vertex_u is None:
        raise Exception("El vertice u no existe")
    vertex_u["value"] = new_info
    ml.put(my_graph["vertices"], key_u, vertex_u)
    return my_graph

def get_vertex_information(my_graph, key_u):
    vertex_u = ml.get(my_graph["vertices"], key_u)
    if vertex_u is None:
        raise Exception("El vertice u no existe")
    return vertex_u["value"]

