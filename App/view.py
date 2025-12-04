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
    lat_o = float(input("Latitud del punto de origen: "))
    lon_o = float(input("Longitud del punto de origen: "))
    lat_d = float(input("Latitud del punto de destino: "))
    lon_d = float(input("Longitud del punto de destino: "))
    radio = float(input("Indique el área de interés (Km): "))
    


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
    pass


def print_req_5(result):
    if "error" in result:
        print("\nError:", result["error"])
        return

    print("\n==============================================")
    print("                REQ 5  RUTA EFICIENTE")
    print("==============================================\n")

    print(f"Nodo origen más cercano: {result['origen_id']}")
    print(f"Nodo destino más cercano: {result['destino_id']}")
    print(f"Costo total del camino: {result['total_cost']}")
    print(f"Total nodos en la ruta: {result['total_nodos']}")
    print(f"Total segmentos: {result['total_segmentos']}\n")

    print("--------------  CAMINO DETALLADO  --------------\n")

    camino = result["camino"]

    for i in range(result["total_nodos"]):
        nodo = camino[i]

        print(f"Nodo {i+1}:")
        print(f"   ID: {nodo['id']}")
        print(f"   Ubicación: ({nodo['lat']}, {nodo['lon']})")
        print(f"   Conteo de grullas: {nodo['conteo']}")
        print(f"   Primeros 3 tags: {nodo['primeros3']}")
        print(f"   Últimos 3 tags: {nodo['ultimos3']}")

        if nodo["dist_next"] is None:
            print("   Distancia al siguiente nodo: N/A (último nodo)")
        else:
            print(f"   Distancia al siguiente nodo: {nodo['dist_next']} km")

        print("-" * 50)

    print("\nFin del reporte del requerimiento 5.\n")



def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    if control is None:
        print("Primero debe cargar la información (opción 0).\n")
        return

    total_subredes, subredes = lg.req_6(control)

    if total_subredes == 0:
        print("No se reconoce ninguna subred hídrica viable dentro del nicho biológico.\n")
        return

    print("==================")
    print("REQUERIMIENTO 6: Subredes hídricas dentro del nicho biológico")
    print("==================")
    print(f"Total de subredes hídricas identificadas: {total_subredes}\n")

    # Tabla resumen de las subredes (top 5)
    tabla_resumen = []
    for comp in subredes:
        fila = {
            "Id subred": comp["id_subred"],
            "# puntos": comp["num_puntos"],
            "Lat min": comp["min_lat"],
            "Lat max": comp["max_lat"],
            "Lon min": comp["min_lon"],
            "Lon max": comp["max_lon"],
            "# grullas": comp["total_grullas"]
        }
        tabla_resumen.append(fila)

    print("--- Subredes hídricas más grandes ---")
    print(tb.tabulate(tabla_resumen, headers="keys", tablefmt="fancy_grid"))
    print()

    # Detalle por cada subred
    for comp in subredes:
        print(f"=== Detalle subred {comp['id_subred']} ===")
        print(f"Puntos migratorios en la subred: {comp['num_puntos']}")
        print(f"Total de individuos (grullas) en la subred: {comp['total_grullas']}")

        # puntos
        print("\nPrimeros 3 puntos migratorios (id, lat, lon):")
        print(tb.tabulate(comp["puntos_prim"], headers="keys", tablefmt="fancy_grid"))

        print("\nÚltimos 3 puntos migratorios (id, lat, lon):")
        print(tb.tabulate(comp["puntos_ult"], headers="keys", tablefmt="fancy_grid"))

        # grullas
        print("\nPrimeros 3 identificadores de grullas:")
        print(comp["grullas_prim"])

        print("Últimos 3 identificadores de grullas:")
        print(comp["grullas_ult"])
        print("\n")


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
