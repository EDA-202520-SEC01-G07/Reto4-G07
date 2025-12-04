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
        "grafo_desplazamiento": None,
        "grafo_hidrico": None
    }
    catalog["eventos"] = al.new_list()
    catalog["grafo_desplazamiento"] = d.new_graph(7000)
    catalog["grafo_hidrico"] = d.new_graph(7000)
    return catalog

# Funciones para la carga de datos

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos
    start = get_time()
    grullas_ident = {}
    # 1. Carga de todos los eventos y organizarlos
    input_file = csv.DictReader(open(filename, encoding= 'utf-8'))
    for evento in input_file:
        e = {"id": evento["event-id"],
            "latitud": float(evento["location-lat"]),
            "longitud": float(evento["location-long"]),
            "timestamp": evento["timestamp"],
            "time_dt": dt.datetime.strptime(evento["timestamp"], "%Y-%m-%d %H:%M:%S.%f"),
            "comments": float(evento["comments"])*0.001, #está en m, pasar a km
            "tag-local-identifier": int(evento["tag-local-identifier"])}
        al.add_last(catalog["eventos"], e)
        if e["tag-local-identifier"] not in grullas_ident:
            grullas_ident[e["tag-local-identifier"]]=[]
    al.merge_sort(catalog["eventos"], al.sort_crit_reto4)
    
    # 2. Construcción vértices
    g_des = catalog["grafo_desplazamiento"]
    g_hid = catalog["grafo_hidrico"]
    llaves = []
    grulla_0 = al.first_element(catalog["eventos"]) #Me da la primera grulla
    primer_vert = nuevo_vertice(grulla_0)
    llaves.append(grulla_0["id"])
    d.insert_vertex(g_des, grulla_0["id"], primer_vert)
    d.insert_vertex(g_hid, grulla_0["id"], primer_vert)
    
    for i in range(1, al.size(catalog["eventos"])):
        grulla = al.get_element(catalog["eventos"], i)
        agregado = False
        for key in llaves:
            nodo = d.get_vertex_information(g_des, key)
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
                grullas_ident[grulla["tag-local-identifier"]].append(key)
                agregado = True
                break
        if not agregado:
            vertice = nuevo_vertice(grulla) #Se crea un nuevo vértice si no está a 3 Km o en el rango de 3 horas
            d.insert_vertex(g_des, grulla["id"], vertice)
            d.insert_vertex(g_hid, grulla["id"], vertice)
            llaves.append(grulla["id"])
            grullas_ident[grulla["tag-local-identifier"]].append(grulla["id"])
    
    """"
    # 3. CREAR ARCOS DE DESPLAZAMIENTO
    migracion = catalog["grafo_desplazamiento"]
    vertices = catalog["vertices"]
    arcos = {}
    for grulla_id in grullas_ident:
        ruta = grullas_ident[grulla_id] # lista con los nodos en orden temporal
        # Debe haber al menos dos nodos para crear un arco
        if len(ruta) >= 2:
            #¿Qué hago acá?
            for i in range(1, len(ruta)):
                vert_A = m.get(vertices, ruta[i-1])
                vert_B = m.get(vertices, ruta[i])
                if vert_A == vert_B:
                    continue
                distancia = h.haversine((m.get(vert_A,"latitud"),m.get(vert_A,"longitud")),(m.get(vert_B,"latitud"),m.get(vert_B,"longitud")))
                diferencia = abs(m.get(vert_B, "time_dt") - m.get(vert_A, "time_dt"))
                horas = diferencia.total_seconds() / 3600
                if arcos[(vert_A["event_id"], vert_B["event_id"])] not in arcos:
                    arcos[vert_A["event_id"], vert_B["event_id"]] = [distancia, 1]
                else:
                    arcos[vert_A["event_id"], vert_B["event_id"]] = [arcos[vert_A["event_id"], vert_B["event_id"]][0]+diferencia, arcos[vert_A["event_id"], vert_B["event_id"]][0]+1]
    for i in arcos:
        A = arcos[i][0]
        print(A)

        prom= round(arcos[i][0]/arcos[i][1],2)
        if not d.contains_vertex(migracion, evento_0["id"]):
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
        for j in range(i+1, len(llaves)):
            nodo_j = m.get(nodos, llaves[j])
            # 1. A < B en tiempo
            if nodo_i["time_dt"] >= nodo_j["time_dt"]:
                continue
            # 2. Comparten grulla
            comparten = False
            for k in range(0, al.size(nodo_i["grullas"]) ):
                g = al.get_element(nodo_i["grullas"], k)
                if m.contains(nodo_j["map_eventos"], g):
                    comparten = True
                    break
            if not comparten:
                continue
            # 3. Distancia <= 10 km
            distancia = h.haversine(nodo_i["location"], nodo_j["location"])
            if distancia > 10:
                continue
            # 4. Diferencia tiempo <= 48 horas
            horas = abs((nodo_j["time_dt"] - nodo_i["time_dt"]).total_seconds()) / 3600
            if horas > 48:
                continue
            # 5. Crear vertices si no existen
            if not d.contains_vertex(grafo_hidrico, nodo_i["event_id"]):
                d.insert_vertex(grafo_hidrico, nodo_i["event_id"], None)
            if not d.contains_vertex(grafo_hidrico, nodo_j["event_id"]):
                d.insert_vertex(grafo_hidrico, nodo_j["event_id"], None)
            # 6. REVISAR SI YA EXISTE ARCO (super eficiente)
            adj_i = d.get_vertex(grafo_hidrico, nodo_i["event_id"])["adjacents"]
            #if m.contains(adj_i, nodo_j["event_id"]):
               # continue
            # 7. Agregar arco con peso redondeado
            d.add_edge(grafo_hidrico, nodo_i["event_id"], nodo_j["event_id"], round(distancia, 2))
    """
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
    vertices = catalog["grafo_desplazamiento"]
    primeros = []
    ultimos = []
    for i in range(5):
        elem = d.get_vertex_information(vertices, llaves[i])
        lat = elem["location"][0]
        long = elem["location"][1]
        datos = {"Identificador único": elem["event_id"],
                "Posición (lat, lon)": (round(lat, 5), round(long, 5)),
                "Fecha de creación": elem["timestamp"],
                "Grullas (Tags)": [elem["grullas"]["elements"][0]],
                "Conteo de eventos": elem["conteo"]}
        primeros.append(datos)
            
    for j in range(d.order(vertices)-5,d.order(vertices)):
        elem = d.get_vertex_information(vertices, llaves[j])
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
