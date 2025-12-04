import sys
import tabulate as tb
from App import logic as lg
from DataStructures.Map import map_separate_chaining as m
from DataStructures.Priority_queue import priority_queue as pq
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
    control = new_logic()
    file = input('Diga el archivo que quiere evaluar (small, large, 30pct, 80pct)\n').strip().lower()
    file = "data/1000_cranes_mongolia_"+file+".csv"
    tiempo, g_ident, llaves = lg.load_data(control, file)
    print("==================")
    print("CARGA DE DATOS")
    print("Tiempo de carga: " + str(round(tiempo,3)))
    print("==================")
    print("Total de grullas reconocidas: "+str(len(g_ident)))
    print("Total de eventos cargados: "+str(pq.size(control["eventos"])))
    print("Total de nodos del grafo: "+str(m.size(control["vertices"])))
    print("Total de arcos en el grafo: " + str(d.size(control["grafo_hidrico"])))
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
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    print("\n========== REQUERIMIENTO 1 ==========\n")

    lat_o = float(input("Latitud del punto de origen: "))
    lon_o = float(input("Longitud del punto de origen: "))
    lat_d = float(input("Latitud del punto de destino: "))
    lon_d = float(input("Longitud del punto de destino: "))
    grulla_id = int(input("Tag-local-identifier de la grulla: "))

    print("\nProcesando...\n")

    respuesta = lg.req_1(control, lat_o, lon_o, lat_d, lon_d, grulla_id)

    # Verificación de errores
    if "error" in respuesta:
        print( respuesta["error"] + "\n")
        return

    # Información básica
    print(" Nodo origen más cercano:", respuesta["origen_id"])
    print(" Nodo destino más cercano:", respuesta["destino_id"])
    print(" Número total de nodos en el camino:", respuesta["total_nodos"])
    print(" Distancia total recorrida:", round(respuesta["total_dist"], 2), "km\n")

    # Construcción de tabla para el camino
    tabla = []
    for nodo in respuesta["camino"]:
        tabla.append({
            "ID Nodo": nodo["id"],
            "Lat": round(nodo["lat"], 5),
            "Lon": round(nodo["lon"], 5),
            "#Eventos": nodo["conteo"],
            "Primeros 3": nodo["primeros3"],
            "Últimos 3": nodo["ultimos3"],
            "Dist. al sig.": round(nodo["dist_next"], 2) if nodo["dist_next"] else "-"
        })

    print("===== CAMINO ENCONTRADO =====")
    print(tb.tabulate(tabla, headers="keys", tablefmt="fancy_grid"))
    print()



def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    pass


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    pass


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    pass


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    pass


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
