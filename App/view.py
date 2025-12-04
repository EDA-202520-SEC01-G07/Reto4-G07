import sys
import tabulate as tb
from App import logic as lg
from DataStructures.Map import map_separate_chaining as m
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.List import array_list as al
from DataStructures.Graph import digraph as d
def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO: Llamar la función de la lógica donde se crean las estructuras de datos
    return lg.new_logic()

def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Carga los datos
    """
    #TODO: Realizar la carga de datos
    file = input('Diga el archivo que quiere evaluar (small, large, 30pct, 80pct)\n').strip().lower()
    file = "data/1000_cranes_mongolia_"+file+".csv"
    tiempo, grullas, llaves = lg.load_data(control, file)
    print("==================")
    print("CARGA DE DATOS")
    print("Tiempo de carga: " + str(round(tiempo,3)))
    print("==================")
    print("Total de grullas reconocidas: "+str(len(grullas)))
    print("Total de eventos cargados: "+str(al.size(control["eventos"])))
    print("Total de nodos del grafo: "+str(d.order(control["grafo_migraciones"])))
    print("Total de arcos en el grafo [migraciones]: " + str(d.size(control["grafo_migraciones"])))
    print("Total de arcos en el grafo [hídrico]: " + str(d.size(control["grafo_hidrico"])))
    print("================== \n")
    primeros, ultimos = lg.presentacion_datos(control, llaves)
    print("--- Primeros 5 nodos ---")
    print(tb.tabulate(primeros, headers="keys",tablefmt="fancy_grid"))
    print("\n--- Últimos 5 nodos ---")
    print(tb.tabulate(ultimos, headers="keys",tablefmt="fancy_grid"))


def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    #TODO: Realizar la función para imprimir un elemento
    pass

def print_req_1(control):
    print("===========REQUERIMIENTO1===========")
    lat_o = float(input("Ingrese la latitud del punto de origen: ").strip())
    lon_o = float(input("Ingrese la longitud del punto de origen: ").strip())
    lat_d = float(input("Ingrese la latitud del punto de destino: ").strip())
    lon_d = float(input("Ingrese la longitud del punto de destino: ").strip())
    grulla_id = int(input("Ingrese el ID del individuo (grulla): ").strip())
    result = lg.req_1(control, lat_o, lon_o, lat_d, lon_d, grulla_id)
    if "error" in result:
        print("Error:", result["error"])
        return
    print("NodoOrigen:", result["origen_id"])
    print("NodoDestino:", result["destino_id"])
    print("PrimerNodoConIndividuo:", result["mensaje_first_node"])
    print("DistanciaTotal:", result["total_dist"])
    print("TotalNodos:", result["total_nodos"])
    print("Primeros5:")
    primeros = result["primeros5"]
    i = 0
    while i < len(primeros):
        nodo = primeros[i]
        print("ID:", nodo["id"])
        print("Lat:", nodo["lat"])
        print("Lon:", nodo["lon"])
        print("Conteo:", nodo["conteo"])
        print("Primeros3:", nodo["primeros3"])
        print("Ultimos3:", nodo["ultimos3"])
        dist = nodo["dist_next"]
        if dist is None:
            print("DistNext:NA")
        else:
            print("DistNext:", dist)
        i = i + 1
    print("Ultimos5:")
    ultimos = result["ultimos5"]
    j = 0
    while j < len(ultimos):
        nodo = ultimos[j]
        print("ID:", nodo["id"])
        print("Lat:", nodo["lat"])
        print("Lon:", nodo["lon"])
        print("Conteo:", nodo["conteo"])
        print("Primeros3:", nodo["primeros3"])
        print("Ultimos3:", nodo["ultimos3"])
        dist = nodo["dist_next"]
        if dist is None:
            print("DistNext:NA")
        else:
            print("DistNext:", dist)
        j = j + 1


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    #lat_o = float(input("Latitud del punto de origen: "))
    #lon_o = float(input("Longitud del punto de origen: "))
    #lat_d = float(input("Latitud del punto de destino: "))
    #lon_d = float(input("Longitud del punto de destino: "))
    #radio = float(input("Indique el área de interés (Km): "))
    lat_o = 27.75878333
    lon_o = 81.3055
    lat_d = 27.75416667
    lon_d = 81.29623333
    radio = 100
    resultado, tiempo = lg.req_2(control, (lat_o, lon_o), (lat_d, lon_d), radio)
    print("--- DETECTAR MOVIMIENTOS MIGRATORIOS ALREDEDOR DE UN ÁREA ---")
    print("Tiempo de ejecución: "+str(round(tiempo, 4)))
    print(resultado)


def print_req_3(control):
    if control is None:
        print("Primero debe cargar la información (opción 0).\n")
        return
    
    resultado = lg.req_3(control)

    if resultado is None:
        print("No se reconoce una ruta migratoria viable dentro del nicho biológico.\n")
        return

    total_puntos, total_individuos, primeros, ultimos = resultado

    print("==================")
    print("REQUERIMIENTO 3: Ruta migratoria dentro del nicho biológico")
    print("==================")
    print(f"Total de puntos migratorios en la ruta: {total_puntos}")
    print(f"Total de individuos (grullas) que usan la ruta: {total_individuos}")
    print("==================\n")

    if len(primeros) > 0:
        print("--- Primeros 5 puntos de la ruta ---")
        print(tb.tabulate(primeros, headers="keys", tablefmt="fancy_grid"))
        print()

    if len(ultimos) > 0:
        print("--- Últimos 5 puntos de la ruta ---")
        print(tb.tabulate(ultimos, headers="keys", tablefmt="fancy_grid"))
        print()




def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    #lat_o = float(input("Latitud del punto de origen: "))
    #lon_o = float(input("Longitud del punto de origen: "))
    lat_o = 27.75878333
    lon_o = 81.3055


def print_req_5(control):
    print("===========REQUERIMIENTO 5===========")
    lat_o=float(input("Ingrese latitud origen: ").strip())
    lon_o=float(input("Ingrese longitud origen: ").strip())
    lat_d=float(input("Ingrese latitud destino: ").strip())
    lon_d=float(input("Ingrese longitud destino: ").strip())
    tipo=input("Grafo (desplazamiento/hidrico): ").strip().lower()
    if tipo!="hidrico": tipo="desplazamiento"
    result=lg.req_5(control,lat_o,lon_o,lat_d,lon_d, tipo)
    if "error" in result:
        print("Error:",result["error"]);return
    print("Origen:",result["origen_id"])
    print("Destino:",result["destino_id"])
    print("CostoTotal:",result["total_cost"])
    print("TotalNodos:",result["total_nodos"])
    print("TotalSegmentos:",result["total_segmentos"])
    print("Primeros5:")
    i=0
    while i < min(5, len(result["camino"])):
        n=result["camino"][i]
        print("ID:",n["id"])
        print("Lat:",n["lat"])
        print("Lon:",n["lon"])
        print("Conteo:",n["conteo"])
        print("Primeros3:",n["primeros3"])
        print("Ultimos3:",n["ultimos3"])
        print("DistNext:",("NA" if n["dist_next"] is None else n["dist_next"]))
        i=i+1
    print("Ultimos5:")
    total=len(result["camino"])
    start= total - min(5,total)
    j=start
    while j< total:
        n=result["camino"][j]
        print("ID:",n["id"])
        print("Lat:",n["lat"])
        print("Lon:",n["lon"])
        print("Conteo:",n["conteo"])
        print("Primeros3:",n["primeros3"])
        print("Ultimos3:",n["ultimos3"])
        print("DistNext:",("NA" if n["dist_next"] is None else n["dist_next"]))
        j=j+1
    print("FinRequerimiento5")




def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    pass

# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 1:
            print_req_1(control)

        elif int(inputs) == 2:
            print_req_2(control)

        elif int(inputs) == 3:
            print_req_3(control)

        elif int(inputs) == 4:
            print_req_4(control)

        elif int(inputs) == 5:
            print_req_5(control)

        elif int(inputs) == 5:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
