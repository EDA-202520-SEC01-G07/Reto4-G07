from DataStructures.Graph import digraph as G
from DataStructures.List import array_list as lt
from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as s
from DataStructures.Map import map_linear_probing as m

def dfs(my_graph, source):
    # grafo que almacenará los visitados
    if not G.contains_vertex(my_graph, source):
        return None
    
    visitados = m.new_map(num_elements=G.order(my_graph), load_factor=0.5,)
    m.put(visitados, source, {"marked": True, "edge_from": None})
    visitados = dfs_vertex(my_graph, source, visitados)
    return visitados

def dfs_vertex(my_graph, vertex_key, visited_dict):
    # Marcar el vértice como visitad

    # Obtener la lista de adyacencia del vértice
    
    adyacentes =G.adjacent(my_graph, vertex_key)
    if adyacentes is None:          
        return visited_dict
    # Recorrer cada vecino
    for i in range(lt.size(adyacentes)):
        vecino = lt.get_element(adyacentes, i)
        visitados = m.get(visited_dict, vecino)
       
        # Si no ha sido visitado
        if visitados is None:
            m.put(visited_dict, vecino, {"marked": True, "edge_from": vertex_key})
            dfs_vertex(my_graph, vecino, visited_dict)  # LLAMADA RECURSIVA
    return visited_dict

def has_path_to(key_v, visited_map):
    return m.contains(visited_map, key_v)

def path_to(key_v, visited_map):
    path=s.new_stack()
    current_key=key_v
    i =m.get(visited_map, key_v)
    if i is None:
        return None
    while current_key is not None:
        s.push(path, current_key)
        current_key = m.get(visited_map, current_key)["edge_from"]
    return path
    
