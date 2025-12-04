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
from DataStructures.Graph import dijkstra as djk
from DataStructures.Stack import stack as s
from DataStructures.Graph import bfs as bfs
from DataStructures.Graph import dfo_structure as dfo
from DataStructures.Graph import digraph as G
from DataStructures.List import single_linked_list as sl
def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    catalog = {
        "eventos": None,
        "grafo_migraciones": None,
        "grafo_hidrico": None
    }
    catalog["eventos"] = al.new_list()
    catalog["grafo_migraciones"] = d.new_graph(6500)
    catalog["grafo_hidrico"] = d.new_graph(6500)
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
    g_mig = catalog["grafo_migraciones"]
    g_hid = catalog["grafo_hidrico"]
    llaves = []
    grulla_0 = al.first_element(catalog["eventos"]) #Me da la primera grulla
    primer_vert = nuevo_vertice(grulla_0)
    llaves.append(grulla_0["id"])
    d.insert_vertex(g_mig, grulla_0["id"], primer_vert)
    d.insert_vertex(g_hid, grulla_0["id"], primer_vert)
    
    for i in range(1, al.size(catalog["eventos"])):
        grulla = al.get_element(catalog["eventos"], i)
        agregado = False
        for key in llaves:
            nodo = d.get_vertex_information(g_mig, key)
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
            d.insert_vertex(g_mig, grulla["id"], vertice)
            d.insert_vertex(g_hid, grulla["id"], vertice)
            llaves.append(grulla["id"])
            grullas_ident[grulla["tag-local-identifier"]].append(grulla["id"])
    
    
    # 3. CREAR ARCOS DE DESPLAZAMIENTO
    arcos = {}
    for grulla_id in grullas_ident:
        ruta = grullas_ident[grulla_id] # lista con los nodos en orden temporal
        # Debe haber al menos dos nodos para crear un arco
        if len(ruta) < 2:
            continue
        
        for i in range(1, len(ruta)):
            vert_A_id = ruta[i-1]
            vert_B_id = ruta[i]
            if vert_A_id == vert_B_id:
                continue
            vA = d.get_vertex_information(g_mig, vert_A_id)["map_eventos"]
            vB = d.get_vertex_information(g_mig, vert_B_id)["map_eventos"]
            llaves_vA = m.key_set(vA)
            llaves_vB = m.key_set(vB)
            suma = 0
            cont = 0
            
            for j in range(al.size(llaves_vA)):
                eventosA = m.get(vA, al.get_element(llaves_vA, j))
                for k in range(al.size(llaves_vB)):
                    eventosB = m.get(vB, al.get_element(llaves_vB, k))
                    dist = h.haversine((eventosA["latitud"],eventosA["longitud"]),(eventosB["latitud"],eventosB["longitud"]))
                    suma += dist
                    cont += 1
            if (vert_A_id, vert_B_id) not in arcos:
                arcos[(vert_A_id, vert_B_id)] = [suma, cont]
            else:
                arcos[(vert_A_id, vert_B_id)][0] += suma
                arcos[(vert_A_id, vert_B_id)][1] += cont
    
    for (nodoA, nodoB) in arcos:
        promedio = round(arcos[(nodoA, nodoB)][0]/arcos[(nodoA, nodoB)][1], 2)
        d.add_edge(g_mig, nodoA, nodoB, promedio)

    # 4. CREAR ARCOS DE PROXIMIDAD HÍDRICA 
    grafo_hidrico = catalog["grafo_hidrico"]

    for grulla_id in grullas_ident:
        ruta = grullas_ident[grulla_id]  # lista de nodos en orden temporal
        if len(ruta) < 2:
            continue

        for i in range(1, len(ruta)):
            vert_A_id = ruta[i-1]
            vert_B_id = ruta[i]

            if vert_A_id == vert_B_id:
                continue

            nodo_A = d.get_vertex_information(g_mig, vert_A_id)
            nodo_B = d.get_vertex_information(g_mig, vert_B_id)

            # Crear vértices en grafo_hidrico si no existen aún
            if not d.contains_vertex(grafo_hidrico, vert_A_id):
                d.insert_vertex(grafo_hidrico, vert_A_id, nodo_A)
            if not d.contains_vertex(grafo_hidrico, vert_B_id):
                d.insert_vertex(grafo_hidrico, vert_B_id, nodo_B)

            # Peso hídrico
            peso = nodo_B["prom_agua"][0]
            d.add_edge(grafo_hidrico, vert_A_id, vert_B_id, peso)



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
    vertices = catalog["grafo_migraciones"]
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

def req_1(catalog, lat_o, lon_o, lat_d, lon_d, grulla_id):
    grafo = catalog["grafo_migraciones"]
    vids = d.vertices(grafo)
    if vids is None or al.size(vids) == 0:
        return {"error":"Grafo vacío."}
    origen = None
    destino = None
    mo = float("inf")
    md = float("inf")
    i = 0
    while i < al.size(vids):
        vid = al.get_element(vids, i)
        info = d.get_vertex_information(grafo, vid)
        if info is not None and "location" in info and info["location"] is not None:
            loc = info["location"]
            do = h.haversine((lat_o, lon_o), loc)
            dd = h.haversine((lat_d, lon_d), loc)
            if do < mo:
                mo = do; origen = info
            if dd < md:
                md = dd; destino = info
        i = i + 1
    if origen is None or destino is None:
        return {"error":"No se encontraron origen/destino cercanos."}
    oid = origen["event_id"] if "event_id" in origen else "Unknown"
    did = destino["event_id"] if "event_id" in destino else "Unknown"
    search = dfs.dfs(grafo, oid)
    path = dfs.path_to(did, search)
    # construir nodos
    nodes = []
    total_dist = 0.0
    idx = 0
    npath = sl.size(path)
    while idx < npath:
        nid = sl.get_element(path, idx)
        info = d.get_vertex_information(grafo, nid)
        lat = "Unknown"; lon = "Unknown"; conteo = "Unknown"
        p3 = ["Unknown","Unknown","Unknown"]
        u3 = ["Unknown","Unknown","Unknown"]
        if info is not None:
            if "location" in info and info["location"] is not None:
                lat = info["location"][0]; lon = info["location"][1]
            conteo = info["conteo"] if "conteo" in info else "Unknown"
            if "grullas" in info and al.size(info["grullas"])>0:
                ng = al.size(info["grullas"])
                ptemp = []
                utemp = []
                j = 0
                while j < min(3, ng):
                    ptemp.append(al.get_element(info["grullas"], j))
                    j = j+1
                k = ng - min(3, ng)
                while k < ng:
                    utemp.append(al.get_element(info["grullas"], k))
                    k = k+1
                while len(ptemp)<3: 
                    ptemp.append("Unknown")
                while len(utemp)<3: 
                    utemp.append("Unknown")
                p3 = ptemp
                u3 = utemp
        dist_next = None
        if idx < npath - 1:
            nxt = sl.get_element(path, idx+1)
            vert = d.get_vertex(grafo, nid)
            print (vert["adjacents"])
            print (nxt)
            if vert is not None and "adjacents" in vert and vert["adjacents"] is not None:
                w = m.get(vert["adjacents"], nxt)
                if w is not None:
                    wf = float(w)
                    dist_next = round(wf,2)
                    total_dist = total_dist + wf
        nodes.append({"id":nid,"lat":lat,"lon":lon,"conteo":conteo,"primeros3":p3,"ultimos3":u3,"dist_next":dist_next})
        idx = idx + 1

    total_n = len(nodes)
    primeros5 = []
    ultimos5 = []
    i = 0
    while i < min(5, total_n):
        primeros5.append(nodes[i]); i = i + 1
    j = total_n - min(5, total_n)
    while j < total_n:
        ultimos5.append(nodes[j]); j = j + 1

    # primer nodo donde aparece la grulla
    first = "Unknown"
    i = 0
    while i < total_n:
        if nodes[i]["primeros3"] is not None:
            k = 0
            while k < 3:
                if nodes[i]["primeros3"][k] == grulla_id:
                    first = nodes[i]["id"]; k = 3; i = total_n
                k = k + 1
        i = i + 1

    mensaje = ("El individuo aparece por primera vez en el nodo: " + str(first)) if first != "Unknown" else "Unknown"

    return {"origen_id":oid,"destino_id":did,"mensaje_first_node":mensaje,"total_dist":round(total_dist,2),"total_nodos":total_n,"primeros5":primeros5,"ultimos5":ultimos5}




def req_2(catalog, p_origen, p_destino, radio):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    migraciones = catalog["grafo_migraciones"]
    movimientos_xAreas = bfs.bfs(migraciones, p_origen)
    
    


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    nicho=catalog["grafo_migraciones"]
    dfo_result =dfo.dfo(nicho)
    orden_topologico = dfo_result["reversepost"]
    ruta_top=[]
    
    while not s.is_empty(orden_topologico):
        ruta_top.append(s.pop(orden_topologico))
    n=len(ruta_top)
    dist={}
    ant={}
    for i in ruta_top:
        dist[i]=1
        ant[i]=None
    mejor=None
    
    for j in ruta_top:
        for k in G.adjacent(nicho,j):
            if dist[j]+1>dist.get(k,0):
                dist[k]=dist[j]+1
                ant[k]=j
        if mejor is None or dist[k]>dist[mejor]:
                    mejor=k
                    
    if mejor is None or dist[mejor] < 2:
        return None
    path=[]
    actual=mejor
    
    while actual is not None:
        path.append(actual)
        actual=ant[actual]
    path.reverse()
    return path


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass



def req_5(catalog, lat_o, lon_o, lat_d, lon_d, grafo_tipo):
    # seleccionar grafo
    if grafo_tipo == "hidrico":
            grafo = catalog["grafo_hidrico"]
    else:
            grafo = catalog["grafo_migraciones"]
    vids = d.vertices(grafo)
    if vids is None or al.size(vids) == 0:
        return {"error":"El grafo no tiene vértices. Ejecute load_data."}

    # encontrar origen y destino más cercanos
    i = 0
    nvid = al.size(vids)
    nodo_or = None
    nodo_de = None
    mo = float("inf")
    md = float("inf")
    while i < nvid:
        vid = al.get_element(vids, i)
        info = d.get_vertex_information(grafo, vid)
        if info is not None and "location" in info and info["location"] is not None:
            loc = info["location"]
            do = h.haversine((lat_o, lon_o), loc)
            dd = h.haversine((lat_d, lon_d), loc)
            if do < mo:
                mo = do; nodo_or = info
            if dd < md:
                md = dd; nodo_de = info
        i = i + 1
    if nodo_or is None or nodo_de is None:
        return {"error":"No se encontraron nodos origen/destino cercanos."}
    source = nodo_or["event_id"] if nodo_or is not None else "Unknown"
    target = nodo_de["event_id"] if nodo_de is not None else "Unknown"
    # verificar que vertices existan
    if not d.contains_vertex(grafo, source):
        return {"error":"Nodo origen no existe en grafo: " + str(source)}
    if not d.contains_vertex(grafo, target):
        return {"error":"Nodo destino no existe en grafo: " + str(target)}
    # Dijkstra
    aux = djk.dijkstra(grafo, source)
    if aux is None:
        return {"error":"Dijkstra falló con el nodo origen."}
    if not djk.has_path_to(target, aux):
        return {"error":"No existe un camino viable entre los puntos."}
    stack_path = djk.path_to(target, aux)
    if stack_path is None:
        return {"error":"No se pudo reconstruir el camino."}
    # convertir stack/estructura a lista normal path_list (0-based)
    path_list = []
    if "elements" in stack_path:
        k = 0
        while k < al.size(stack_path):
            path_list.append(al.get_element(stack_path, k))
            k = k + 1
    elif "first" in stack_path:
        node = stack_path["first"]
        while node is not None:
            path_list.append(node["info"])
            node = node["next"]
    else:
        return {"error":"Estructura de path desconocida."}
    # asegurar orden source->...->target
    if len(path_list) > 0:
        if path_list[0] != source and path_list[-1] == source:
            # invertir
            path_list = list(reversed(path_list))
    # construir camino con info de cada nodo
    camino = []
    total_cost = djk.dist_to(target, aux) if hasattr(djk, "dist_to") else 0.0
    # como fallback sumar pesos
    total_cost = float(total_cost) if total_cost is not None else 0.0
    idx = 0
    npath = len(path_list)
    while idx < npath:
        nid = path_list[idx]
        info = d.get_vertex_information(grafo, nid)
        lat = "Unknown"; lon = "Unknown"; conteo = "Unknown"
        p3 = ["Unknown","Unknown","Unknown"]; u3 = ["Unknown","Unknown","Unknown"]
        if info is not None:
            if "location" in info and info["location"] is not None:
                lat = info["location"][0]; lon = info["location"][1]
            conteo = info["conteo"] if "conteo" in info else "Unknown"
            if "grullas" in info and al.size(info["grullas"]) > 0:
                ng = al.size(info["grullas"])
                # primeros3
                ptemp = []
                j = 0
                lim = min(3, ng)
                while j < lim:
                    ptemp.append(al.get_element(info["grullas"], j))
                    j = j + 1
                # ultimos3
                utemp = []; start = ng - min(3, ng); k = start
                while k < ng:
                    utemp.append(al.get_element(info["grullas"], k))
                    k = k + 1
                while len(ptemp) < 3: ptemp.append("Unknown")
                while len(utemp) < 3: utemp.append("Unknown")
                p3 = ptemp; u3 = utemp
        dist_next = None
        if idx < npath - 1:
            nxt = path_list[idx + 1]
            edges_map = d.edges_vertex(grafo, nid)   
            if edges_map is not None:
                edge_obj = m.get(edges_map, nxt) 
                if edge_obj is not None and "weight" in edge_obj:
                    wf = float(edge_obj["weight"])
                    dist_next = round(wf, 2)
        nodo_info = {
            "id": nid,
            "lat": lat,
            "lon": lon,
            "conteo": conteo,
            "primeros3": p3,
            "ultimos3": u3,
            "dist_next": dist_next
        }
        camino.append(nodo_info)
        idx = idx + 1
    total_nodos = len(camino)
    total_segmentos = total_nodos - 1 if total_nodos > 0 else 0
    total_cost = round(total_cost, 2)
    return {"origen_id": source, "destino_id": target, "total_cost": total_cost, "total_nodos": total_nodos, "total_segmentos": total_segmentos, "camino": camino}


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
