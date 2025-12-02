import time
import datetime as dt
import csv
csv.field_size_limit(2147483647)
from DataStructures.Graph import digraph as d
from DataStructures.Map import map_separate_chaining as m
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.List import array_list as al

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    catalog = {
        "eventos": None,
        "vértices": None,
        "eventos_y_nodos": None,
        "grafo_desplazamiento": None,
        "grafo_hidrico": None
    }
    catalog["eventos"] = pq.new_heap(True)
    catalog["vértices"] = m.new_map(1000, 7)
    catalog["eventos_y_nodos"] = m.new_map(22600, 7)
    catalog["grafo_desplazamiento"] = d.new_graph(1000)
    catalog["grafo_hidrico"] = d.new_graph(1000)
    return catalog

# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos
    start = get_time()
    lista = catalog["eventos"]
    input_file = csv.DictReader(open(filename, encoding= 'utf-8'))
    for evento in input_file:
        e = {"id": evento["event-id"],
            "latitud": evento["location-lat"],
            "longitud": evento["location-long"],
            "timestamp": evento["timestamp"],
            "comments": evento["comments"],
            "tag-local-identifier": evento["tag-local-identifier"]}
        pq.insert(lista, dt.datetime.strptime(e["timestamp"],"%Y-%m-%d %H:%M:%S.%f"), e)
    catalog["eventos"] =lista
    end = get_time()
    tiempo = delta_time(start, end)
    return tiempo

# Funciones de consulta sobre el catálogo


def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
