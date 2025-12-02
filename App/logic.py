import time
import datetime as dt
import csv
import haversine as h
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
    catalog["vértices"] = m.new_map(300, 7) #300 porque en el ejemplo salieron menos de 250 en small
    #catalog["eventos_y_nodos"] = m.new_map(22600, 7) 
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
    # 1. Carga de todos los eventos
    cola_prioridad = catalog["eventos"]
    input_file = csv.DictReader(open(filename, encoding= 'utf-8'))
    for evento in input_file:
        e = {"id": evento["event-id"],
            "latitud": float(evento["location-lat"]),
            "longitud": float(evento["location-long"]),
            "timestamp": evento["timestamp"],
            "comments": float(evento["comments"])*0.001, #está en m, pasar a km
            "tag-local-identifier": int(evento["tag-local-identifier"])}
        pq.insert(cola_prioridad, dt.datetime.strptime(e["timestamp"],"%Y-%m-%d %H:%M:%S.%f"), e)
    catalog["eventos"] =cola_prioridad
    
    # 2. Construcción vértices
    """
    Se guardan vertices totales en el mapa "véritces" del catalogo
    Cada vértice tiene:
    1. Id del evento
    2. (Latitud, Longitud)
    3. Timestamp
    4. Eventos de grullas (MAPA donde está la info de las grullas)
    5. Promedio de agua: en forma tupla (promedio, suma, totales) donde la suma y totales son para actualizar el prom
    La información del 1 al 3 las da la primera grulla
    4 y 5 se van actualizando con cada grulla
    """
    nodos = catalog["vértices"] #Es un mapa
    grulla_0 = pq.remove(cola_prioridad) #Me da la primera grulla
    if m.is_empty(nodos): #Guardar info de al menos un evento para empezar a comparar
        mapa=m.new_map(5,7)
        m.put(mapa, "event_id", grulla_0["id"])
        m.put(mapa, "location", (grulla_0["latitud"], grulla_0["longitud"])) #Tupla (Lat, Long)
        m.put(mapa, "tiempo", grulla_0["timestamp"])
        m_grullas = m.new_map(200, 7)
        m.put(m_grullas, "id_grulla", grulla_0["tag-local-identifier"])
        m.put(mapa, "map_eventos", m_grullas)
        promedio = (grulla_0["comments"], grulla_0["comments"], 1)
        m.put(mapa, "prom_agua", promedio)
        m.put(nodos, grulla_0["id"], mapa) #Mete en el mapa de vértices el primero. La llave es el id del evento
        
    grulla = pq.remove(cola_prioridad)
    while not pq.is_empty(cola_prioridad):
        for i in m.key_set(nodos)["elements"]:
            nodo = m.get(nodos, i)
            distancia = h.haversine((grulla["latitud"], grulla["longitud"]), (nodo["location"]))
            t1 = dt.datetime.strptime(grulla["tiempo"],"%Y-%m-%d %H:%M:%S.%f")
            t2 = dt.datetime.strptime(nodo["timestamp"],"%Y-%m-%d %H:%M:%S.%f")
            diferencia = abs(t1-t2)
            horas = diferencia.total_seconds() / 3600
            if distancia < 3 and horas < 3:
            
    
    
    end = get_time()
    tiempo = delta_time(start, end)
    return tiempo, grulla

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
