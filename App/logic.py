import time
import datetime as dt
import csv
import haversine as h
csv.field_size_limit(2147483647)
from DataStructures.Graph import digraph as d
from DataStructures.Graph import dfs as dfs
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
        "event_vertices": None, 
        "vertices": None,
        "grafo_desplazamiento": None,
        "grafo_hidrico": None
    }
    catalog["eventos"] = pq.new_heap(True)
    catalog["event_vertices"] = pq.new_heap(True)
    catalog["vertices"] = m.new_map(300, 7) #300 porque en el ejemplo salieron menos de 250 en small
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
    grullas_ident = []
    # 1. Carga de todos los eventos
    input_file = csv.DictReader(open(filename, encoding= 'utf-8'))
    for evento in input_file:
        e = {"id": evento["event-id"],
            "latitud": float(evento["location-lat"]),
            "longitud": float(evento["location-long"]),
            "timestamp": evento["timestamp"],
            "time_dt": dt.datetime.strptime(evento["timestamp"], "%Y-%m-%d %H:%M:%S.%f"),
            "comments": float(evento["comments"])*0.001, #está en m, pasar a km
            "tag-local-identifier": int(evento["tag-local-identifier"])}
        pq.insert(catalog["event_vertices"], e["time_dt"],e)
        pq.insert(catalog["eventos"], e["time_dt"],e)
        if e["tag-local-identifier"] not in grullas_ident:
            grullas_ident.append(e["tag-local-identifier"])
            
    # 2. Construcción vértices
    llaves = []
    nodos = catalog["vertices"] #Es un mapa
    grulla_0 = pq.remove(catalog["event_vertices"]) #Me da la primera grulla
    primer_vert = nuevo_vertice(grulla_0)
    m.put(nodos, grulla_0["id"], primer_vert) #Mete en el mapa de vértices el primero. La llave es el id del evento
    llaves.append(grulla_0["id"])
    
    while not pq.is_empty(catalog["event_vertices"]):
        grulla = pq.remove(catalog["event_vertices"])
        agregado = False
        for key in llaves:
            nodo = m.get(nodos, key)
            distancia = h.haversine((grulla["latitud"], grulla["longitud"]), nodo["location"])
            diferencia = abs(grulla["time_dt"]-nodo["time_dt"])
            horas = diferencia.total_seconds()/3600
            if distancia < 3 and horas < 3:
                #meter la nueva grulla en el vértice
                al.add_last(nodo["grullas"], grulla["tag-local-identifier"])
                m.put(nodo["map_eventos"], grulla["tag-local-identifier"], grulla)
                nodo["conteo"] = nodo["conteo"]+1
                prom = nodo["prom_agua"]
                prom[1] += grulla["comments"]
                prom[0] = prom[1]/nodo["conteo"]
                agregado = True
                break
        if not agregado:
            vertice = nuevo_vertice(grulla) #Se crea un nuevo vértice si no está a 3 Km o en el rango de 3 horas
            m.put(nodos, grulla["id"], vertice)
            llaves.append(grulla["id"])
            
    # 3. CREAR ARCOS DE DESPLAZAMIENTO
    grafo_desplazamiento = catalog["grafo_desplazamiento"]
    for grulla_id in grullas_ident:
        eventos_grulla = al.new_list()
        # Recolectar los eventos en orden temporal
        for key in llaves:
            nodo = m.get(nodos, key)
            if m.contains(nodo["map_eventos"], grulla_id):
                evento = m.get(nodo["map_eventos"], grulla_id)
                al.add_last(eventos_grulla, evento)
        tam = al.size(eventos_grulla)
        # Debe haber al menos dos eventos para crear un arco
        if tam >= 2:
            for i in range(0, tam - 1):
                evento_0 = al.get_element(eventos_grulla, i)
                evento_1 = al.get_element(eventos_grulla, i + 1)
                distancia = h.haversine((evento_0["latitud"], evento_0["longitud"]),(evento_1["latitud"], evento_1["longitud"]))
                diferencia = abs(evento_1["time_dt"] - evento_0["time_dt"])
                horas = diferencia.total_seconds() / 3600
                if horas > 0:
                    velocidad = distancia / horas
                    # Crear vértices si no existen
                    if not d.contains_vertex(grafo_desplazamiento, evento_0["id"]):
                        d.insert_vertex(grafo_desplazamiento, evento_0["id"], None)
                    if not d.contains_vertex(grafo_desplazamiento, evento_1["id"]):
                        d.insert_vertex(grafo_desplazamiento, evento_1["id"], None)
                    # Agregar arco
                    d.add_edge(grafo_desplazamiento, evento_0["id"], evento_1["id"], velocidad)
    catalog["grafo_desplazamiento"] = grafo_desplazamiento
    # 4. CREAR ARCOS DE PROXIMIDAD HÍDRICA
    grafo_hidrico = catalog["grafo_hidrico"]
    for i in range(len(llaves)):
        nodo_i = m.get(nodos, llaves[i])
        for j in range(i + 1, len(llaves)):
            nodo_j = m.get(nodos, llaves[j])
            distancia = h.haversine(nodo_i["location"], nodo_j["location"])
            diferencia = abs(nodo_j["time_dt"] - nodo_i["time_dt"])
            horas = diferencia.total_seconds() / 3600
            if distancia <= 10 and horas <= 48:
                # Crear vértices si no existen
                if not d.contains_vertex(grafo_hidrico, nodo_i["event_id"]):
                    d.insert_vertex(grafo_hidrico, nodo_i["event_id"], None)
                if not d.contains_vertex(grafo_hidrico, nodo_j["event_id"]):
                    d.insert_vertex(grafo_hidrico, nodo_j["event_id"], None)
                # Agregar arco con peso 1 (puede ser cualquier valor ya que solo interesa la conexión)
                d.add_edge(grafo_hidrico, nodo_i["event_id"], nodo_j["event_id"], 1)
                d.add_edge(grafo_hidrico, nodo_j["event_id"], nodo_i["event_id"], 1)
    end = get_time()
    tiempo = delta_time(start, end)
    return tiempo, grullas_ident, llaves
                
def nuevo_vertice(grulla_0):
    """
    Se guardan vertices totales en el mapa "véritces" del catalogo
    Cada vértice tiene:
    1. Id del evento
    2. (Latitud, Longitud)
    3. Timestamp
    4. Id de grullas
    5. Eventos de grullas (MAPA donde está la info de las grullas)
    6. Conteo de grullas
    7. Promedio de agua: en forma tupla (promedio, suma, totales) donde la suma y totales son para actualizar el prom
    La información del 1 al 3 las da la primera grulla
    4, 5, 6 y 7 se van actualizando con cada grulla
    """
    mapa={}
    mapa["event_id"] = grulla_0["id"]
    mapa["location"]= (grulla_0["latitud"], grulla_0["longitud"]) #Tupla (Lat, Long)
    mapa["timestamp"] = grulla_0["timestamp"]
    mapa["time_dt"] = grulla_0["time_dt"]
    l = al.new_list()
    al.add_last(l, grulla_0["tag-local-identifier"])
    mapa["grullas"] = l
    
    m_grullas = m.new_map(200, 7)
    m.put(m_grullas, grulla_0["tag-local-identifier"], grulla_0)
    mapa["map_eventos"]= m_grullas
    
    mapa["conteo"] = 1
    
    promedio = [grulla_0["comments"], grulla_0["comments"]] #Lista con [promedio, suma] el promedio se hace con el conteo
    mapa["prom_agua"] = promedio
    return mapa

def presentacion_datos(catalog, llaves):
    vertices = catalog["vertices"]
    primeros = []
    ultimos = []
    for i in range(5):
        elem = m.get(vertices, llaves[i])
        lat = elem["location"][0]
        long = elem["location"][1]
        datos = {"Identificador único": elem["event_id"],
                "Posición (lat, lon)": (round(lat, 5), round(long, 5)),
                "Fecha de creación": elem["timestamp"],
                "Grullas (Tags)": [elem["grullas"]["elements"][0]],
                "Conteo de eventos": elem["conteo"]}
        primeros.append(datos)
            
    for j in range(m.size(vertices)-5,m.size(vertices)):
        elem = m.get(vertices, llaves[j])
        lat = elem["location"][0]
        long = elem["location"][1]
        datos = {"Identificador único": elem["event_id"],
                "Posición (lat, lon)": (round(lat, 5), round(long, 5)),
                "Fecha de creación": elem["timestamp"],
                "Grullas (Tags)": [elem["grullas"]["elements"][0]],
                "Conteo de eventos": elem["conteo"]}
        ultimos.append(datos)
    return primeros, ultimos

# Funciones de consulta sobre el catálogo


def req_1(catalog, id_grulla, tiempo_inicio, tiempo_final):
    """
    Retorna el resultado del requerimiento 1
    """
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
