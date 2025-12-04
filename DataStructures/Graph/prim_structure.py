from DataStructures.Map import map_linear_probing as map
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Queue import queue as q
from DataStructures.Graph import digraph as d
from DataStructures.Graph import dfs as dfs
from DataStructures.List import array_list as al
from DataStructures.Graph import dijkstra as djk
from DataStructures.Stack import stack as s
from DataStructures.Graph import bfs as bfs
from DataStructures.Map import map_separate_chaining as m

def new_prim_structure(source, g_order):
    """
    Crea una estructura de busqueda usada en el algoritmo **prim**.

    Se crea una estructura de busqueda con los siguientes atributos:

    - **source**: Vertice de inicio del MST.
    - **edge_from**: Mapa con los vertices visitados. Se inicializa en ``None``
    - **dist_to**: Mapa con las distancias a los vertices. Se inicializa en ``None``
    - **marked**: Mapa con los vertices visitados. Se inicializa en ``None``
    - **pq**: Cola de prioridad indexada (index_priority_queue). Se inicializa en ``None``

    :returns: Estructura de busqueda
    :rtype: prim_search
    """

    structure = {
        "source": source,
        "edge_from": map.new_map(g_order, 0.5),
        "dist_to": map.new_map(g_order, 0.5),
        "marked": map.new_map(g_order, 0.5),
        "pq":  pq.new_heap(),
    }

    return structure


def prim(grafo, origen):
    prim_struct = new_prim_structure(origen, d.order(grafo))

    llaves = d.vertices(grafo)
    for i in range(al.size(llaves)):
        v_id = al.get_element(llaves, i)
        prim_struct["dist_to"][v_id] = float("inf")
        prim_struct["edge_from"][v_id] = None
        prim_struct["marked"][v_id] = False

    prim_struct["dist_to"][origen] = 0
    pq.insert(prim_struct["pq"],0, origen)

    while not pq.is_empty(prim_struct["pq"]):
        u = pq.remove(prim_struct["pq"])
        prim_struct["marked"][u] = True

        vertice_u = d.get_vertex(grafo, u)
        adjacentes = vertice_u["adjacents"]
        keys = m.key_set(adjacentes)

        for j in range(al.size(keys)):
            v = al.get_element(keys, j)
            peso = m.get(adjacentes, v)

            if not prim_struct["marked"][v]:
                if peso < prim_struct["dist_to"][v]:
                    prim_struct["dist_to"][v] = peso
                    prim_struct["edge_from"][v] = u
                    if pq.contains(prim_struct["pq"], v):
                        pq.improve_priority(prim_struct["pq"], peso, v)
                    else:
                        pq.insert(prim_struct["pq"], peso, v)
    return prim_struct

def conexiones_costo(grafo, prim):
    nodos = {"edges": [],
             "vertice": [],
             "dist_to": []}
    peso_total = 0
    llaves = d.vertices(grafo)
    for i in range(al.size(llaves)):
        vertice = al.get_element(llaves, i)
        u = prim["edge_from"][vertice]
        if u is None:
            continue
        peso_total += prim["dist_to"][vertice]
        nodos["edges"].append(u)
        nodos["vertice"].append(vertice)
        nodos["dist_to"].append(prim["dist_to"][vertice])
    nodos["peso_total"] = peso_total
    return nodos