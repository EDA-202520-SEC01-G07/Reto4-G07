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
from DataStructures.Priority_queue import priority_queue as pqe
from DataStructures.Stack import stack as s
from DataStructures.Graph import bfs as bfs
from DataStructures.Graph import dfo_structure as dfo


def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    catalog = {
        "eventos": None,
        "vertices_llaves": None,
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
    v = d.get_vertex(g_mig, grulla_0["id"])

    catalog["vertices_llaves"] = llaves
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
    v = d.get_vertex(g_mig, grulla_0["id"])
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
    llaves = d.vertices(grafo)

    # 1. Encontrar nodo origen más cercano
    nodo_origen = None
    min_dist_origen = float("inf")

    for i in range(1, al.size(llaves) + 1):
        key = al.get_element(llaves, i)
        nodo = d.get_vertex_information(grafo, key)
        dist = h.haversine((lat_o, lon_o), nodo["location"])

        if dist < min_dist_origen:
            min_dist_origen = dist
            nodo_origen = nodo

    # 2. Encontrar nodo destino más cercano
    nodo_destino = None
    min_dist_destino = float("inf")

    for i in range(1, al.size(llaves) + 1):
        key = al.get_element(llaves, i)
        nodo = d.get_vertex_information(grafo, key)
        dist = h.haversine((lat_d, lon_d), nodo["location"])

        if dist < min_dist_destino:
            min_dist_destino = dist
            nodo_destino = nodo
    if nodo_origen is None:
        return {"error": "No se encontró un nodo de origen cercano."}

    if nodo_destino is None:
        return {"error": "No se encontró un nodo de destino cercano."}

    # 3. Verificar que la grulla esté en el nodo origen
    if not m.contains(nodo_origen["map_eventos"], grulla_id):
        return {"error": "El individuo no aparece en este nicho biológico."}

    # 4. DFS desde el nodo de origen
    search = dfs.dfs(grafo, nodo_origen["event_id"])

    if not dfs.has_path_to(search, nodo_destino["event_id"]):
        return {"error": "No existe una ruta viable entre los puntos."}

    # Obtiene el camino de IDs
    path_ids = dfs.path_to(search, nodo_destino["event_id"])

    # 5. Construir toda la información del camino
    total_dist = 0
    nodos_camino = []

    for i in range(len(path_ids)):
        id_nodo = path_ids[i]
        nodo = d.get_vertex_information(grafo, id_nodo)

        lista_grullas = nodo["grullas"]
        n = al.size(lista_grullas)

        primeros3 = []
        ultimos3 = []

        for j in range(1, min(4, n + 1)):
            primeros3.append(al.get_element(lista_grullas, j))

        for j in range(max(1, n - 2), n + 1):
            ultimos3.append(al.get_element(lista_grullas, j))

        dist_next = None
        if i < len(path_ids) - 1:
            siguiente = path_ids[i + 1]
            vert_actual = m.get(grafo["vertices"], id_nodo)
            dist_next = m.get(vert_actual["adjacents"], siguiente)

            if dist_next is not None:
                total_dist += dist_next

        nodos_camino.append({
            "id": id_nodo,
            "lat": nodo["location"][0],
            "lon": nodo["location"][1],
            "conteo": nodo["conteo"],
            "primeros3": primeros3,
            "ultimos3": ultimos3,
            "dist_next": dist_next
        })

    return {
        "origen_id": nodo_origen["event_id"],
        "destino_id": nodo_destino["event_id"],
        "total_dist": total_dist,
        "total_nodos": len(path_ids),
        "camino": nodos_camino
    }


def req_2(catalog, p_origen, p_destino, radio):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    start=get_time()
    migraciones = catalog["grafo_migraciones"]
    vertices = catalog["vertices_llaves"]
    nodo_origen = None
    minO = 99999999999999
    nodo_destino = None
    minD = 99999999999999
    
    for i in vertices:
        nodo = d.get_vertex_information(migraciones, i)
        distancia = h.haversine((p_origen[0], p_origen[1]), nodo["location"])
        if distancia < minO:
            nodo_origen = i
            minO = distancia
        distancia = h.haversine((p_destino[0], p_destino[1]), nodo["location"])
        if distancia < minD:
            nodo_destino = i
            minD = distancia
    if nodo_origen is not None or nodo_destino is not None:
        caminosMigraciones = bfs.bfs(migraciones, nodo_origen)
    else:
        return "No se pudo determinar nodo origen o destino."
    camino = bfs.path_to(nodo_destino, caminosMigraciones)
    if camino is None:
        return "No existe una ruta viable entre los puntos."
    
    resultado = {}
    dist_total = 0
    ultimo = None
    while not s.is_empty(camino):
        elem = s.pop(camino)
        vert = d.get_vertex_information(migraciones, elem)
        mapa_bfs = m.get(caminosMigraciones, elem)
        if mapa_bfs is None:
            continue
        dist_to = mapa_bfs["dist_to"]
        if dist_to <= radio:    
            ultimo = vert
            resultado[vert["id"]] = {"Location": vert["location"],
                "Grullas": vert["grullas"],
                "Primeros 3": vert["grullas"][0:3],
                "Últimos 3": vert["grullas"][-1:-4],
                "dist_to": dist_to}
        else:
            continue
        
    r = {"Ultimo en radio: ":ultimo,
         "Distancia total: ": round(dist_total, 3),
         "Total nodos": len(resultado),
         "Camino":resultado}
    end=get_time()
    tiempo = delta_time(start, end)
    return r, tiempo

def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    nicho=catalog["grafo_migraciones"]
    visitados=m.new_map(d.order(nicho), 7)
    pila_dfs=s.new_stack()
    pila_top=s.new_stack()
    vertices=d.vertices(nicho)
    n=al.size(vertices)
    for i in range(n):
        ve=al.get_element(vertices,i)
        if m.get(visitados,ve) is None:
            s.push(pila_dfs,(ve,0))
    
    while not s.is_empty(pila_dfs):
        nodo,est=s.pop(pila_dfs)
        if est==0:
            if m.get(visitados,nodo) is None:
                m.put(visitados,nodo,{"marked":True,"edge_from":None})
                s.push(pila_dfs,(nodo,1))
                adyacentes=d.adjacent(nicho,nodo)
                if adyacentes is not None:
                    for j in range(al.size(adyacentes)-1,-1,-1):
                        vecino=al.get_element(adyacentes,j)
                        if m.get(visitados,vecino) is None:
                            s.push(pila_dfs,(vecino,0))
        else:
            s.push(pila_top,nodo)
    orden_top=[]
    while not s.is_empty(pila_top):
        orden_top.append(s.pop(pila_top))
    if len (orden_top)==0:
        return None
    
    dist=m.new_map(d.order(nicho),7)
    ant=m.new_map(d.order(nicho),7)
    for v in orden_top:
        m.put(dist,v,1)
        m.put(ant,v,None)
    mejor_fin=None
    mejor_fin
    
    for u in orden_top:
        du=m.get(dist,u)
        adyacentes=d.adjacent(nicho,u)
        if adyacentes is not None:
            for j in range(al.size(adyacentes)):
                v=al.get_element(adyacentes,j)
                dv=m.get(dist,v)
                if dv is None:
                    dv=1
                    m.put(dist,v,dv)
                    m.put(ant,v,None)
                if du+2>dv:
                    m.put(dist,v,du+1)
                    m.put(ant,v,u)
        du_act=m.get(dist,u)
        if du_act>mejor_long:
            mejor_long=du_act
            mejor_fin=u
    
    if mejor_fin is None or mejor_long<2:
        return None
    camino_rev=[]
    actual=mejor_fin
    while actual is not None:
        camino_rev.append(actual)
        actual=m.get(ant,actual)
    
    camino=[]
    for i in range(len(camino_rev)-1,-1,-1):
        camino.append(camino_rev[i])
    
    tot_puntos=len(camino)
    
    individuos=set()
    for v_id in camino:
        info_v=d.get_vertex_information(nicho,v_id)
        lista_grullas=info_v["grullas"]
        for k in range(al.size(lista_grullas)):
            individuos.add(al.get_element(lista_grullas,k))
    tot_individuos=len(individuos)
    primeros=[]
    ultimos=[]
    
    if tot_puntos<5:
        limite=tot_puntos
    else:
        limite=5
    for i in range(limite):
        v_id=camino[i]
        info_v=d.get_vertex_information(nicho,v_id)
        lat=info_v["location"][0]
        long=info_v["location"][1]
        datos={"Identificador único":info_v["event_id"],
               "Posición (lat, lon)":(round(lat,5),round(long,5)),
               "Fecha de creación":info_v["timestamp"],
               "Grullas (Tags)":info_v["grullas"]["elements"],
               "Conteo de eventos":info_v["conteo"]}
        primeros.append(datos)
    for i in range(tot_puntos-limite,tot_puntos):
        v_id=camino[i]
        info_v=d.get_vertex_information(nicho,v_id)
        lat=info_v["location"][0]
        long=info_v["location"][1]
        datos={"Identificador único":info_v["event_id"],
               "Posición (lat, lon)":(round(lat,5),round(long,5)),
               "Fecha de creación":info_v["timestamp"],
               "Grullas (Tags)":info_v["grullas"]["elements"],
               "Conteo de eventos":info_v["conteo"]}
        ultimos.append(datos)
    return tot_puntos,tot_individuos,primeros,ultimos
def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    migraciones = catalog["grafo_hidrico"]
    vertices = catalog["vertices_llaves"]
    nodo_origen = None
    minO = 99999999999999
    
    for i in vertices:
        nodo = d.get_vertex_information(migraciones, i)
        distancia = h.haversine((p_origen[0], p_origen[1]), nodo["location"])
        if distancia < minO:
            nodo_origen = i
            minO = distancia



def req_5(catalog, lat_o, lon_o, lat_d, lon_d, grafo_tipo="migraciones"):

    nodos = catalog["grafo_migraciones"]

    # escoger grafo correcto
    if grafo_tipo == "hidrico":
        grafo = catalog["grafo_hidrico"]
    else:
        grafo = catalog["grafo_migraciones"]

    llaves = m.key_set(nodos)
    if llaves is None or al.size(llaves) == 0:
        return {"error": "No hay nodos en el catálogo."}


    # 1. ENCONTRAR NODO ORIGEN Y DESTINO MÁS CERCANOS

    nodo_origen = None
    nodo_destino = None
    min_origen = 999999999999999999999999
    min_destino = 999999999999999999999999

    for i in range(1, al.size(llaves) + 1):
        key = al.get_element(llaves, i)
        nodo = m.get(nodos, key)

        loc = nodo["location"]
        dist_o = h.haversine((lat_o, lon_o), loc)
        dist_d = h.haversine((lat_d, lon_d), loc)

        if dist_o < min_origen:
            min_origen = dist_o
            nodo_origen = nodo

        if dist_d < min_destino:
            min_destino = dist_d
            nodo_destino = nodo

    if nodo_origen is None:
        return {"error": "No existe nodo origen cercano."}
    if nodo_destino is None:
        return {"error": "No existe nodo destino cercano."}

    source = nodo_origen["event_id"]
    target = nodo_destino["event_id"]

    # 2. CORRER DIJKSTRA 
    dijk = djk.dijkstra(grafo, source)
    if dijk is None:
        return {"error": "El nodo origen no existe en el grafo."}

    if not djk.has_path_to(target, dijk):
        return {"error": "No existe un camino entre los puntos."}


    # 3. RECONSTRUIR PATH

    stack_path = djk.path_to(target, dijk)

    path_ids = []
    while not s.is_empty(stack_path):
        x = s.pop(stack_path)
        path_ids.append(x)

    total_cost = round(djk.dist_to(target, dijk), 2)


    # 4. CONSTRUIR INFORMACIÓN DE NODOS

    camino = []
    total_nodos = len(path_ids)

    for i in range(total_nodos):
        node_id = path_ids[i]
        nodo = m.get(nodos, node_id)

        lista = nodo["grullas"]
        n = al.size(lista)

        primeros3 = []
        ultimos3 = []

        for j in range(1, min(4, n + 1)):
            primeros3.append(al.get_element(lista, j))

        for j in range(max(1, n - 2), n + 1):
            ultimos3.append(al.get_element(lista, j))

        # distancia al siguiente
        dist_next = None
        if i < total_nodos - 1:
            siguiente = path_ids[i + 1]
            vert = d.get_vertex(grafo, node_id)
            ady = vert["adjacents"]
            peso = m.get(ady, siguiente)
            if peso is not None:
                dist_next = round(float(peso), 2)

        camino.append({
            "id": node_id,
            "lat": nodo["location"][0],
            "lon": nodo["location"][1],
            "conteo": nodo["conteo"],
            "primeros3": primeros3,
            "ultimos3": ultimos3,
            "dist_next": dist_next
        })

    return {
        "origen_id": source,
        "destino_id": target,
        "total_cost": total_cost,
        "total_nodos": total_nodos,
        "total_segmentos": total_nodos - 1,
        "camino": camino
    }


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
