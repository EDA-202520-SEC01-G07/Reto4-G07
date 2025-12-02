from DataStructures.Map import map_linear_probing as mp
from DataStructures.List import array_list as lt
from DataStructures.Map import map_entry as me
from DataStructures.Map import map_functions as mf
from DataStructures.Utils.utils import handle_not_implemented


def setup_tests(scale, shift):
    new_map = mp.new_map(5, 0.5, 7)
    if scale is not None and shift is not None:
        new_map["scale"] = scale
        new_map["shift"] = shift
    return new_map


@handle_not_implemented
def test_new_map():
    map = mp.new_map(5, 0.5, 7)
    assert map["prime"] == 7
    assert map["capacity"] == 11
    assert map["scale"] >= 1 and map["scale"] < 7
    assert map["shift"] >= 0 and map["shift"] < 7
    assert map["table"] is not None
    assert map["current_factor"] == 0
    assert map["limit_factor"] == 0.5
    assert map["size"] == 0
    assert map["type"] == "PROBING"

    map = mp.new_map(10, 0.5)
    assert map["prime"] == 109345121


@handle_not_implemented
def test_put():
    map = setup_tests(None, None)
    mp.put(map, 1, 2)
    assert map["size"] == 1
    mp.put(map, 1, 3)
    assert map["size"] == 1
    mp.put(map, 2, 3)
    assert map["size"] == 2

    new_map = mp.new_map(5, 0.5, 7)
    for i in range(5):
        mp.put(new_map, i, i)

    assert new_map["size"] == 5
    assert new_map["capacity"] == 11

    mp.put(new_map, 5, 5)

    assert new_map["size"] == 6
    assert new_map["capacity"] == 23


@handle_not_implemented
def test_contains():
    map = setup_tests(None, None)
    mp.put(map, 1, 2)

    # Test para el caso en que la llave existe
    assert mp.contains(map, 1)

    # Test para el caso en que la llave no existe
    assert not mp.contains(map, 2)

    # Test para el caso en que la tabla está vacía
    new_map = mp.new_map(5, 0.5, 7)
    assert not mp.contains(new_map, 1)


@handle_not_implemented
def test_get():
    map = setup_tests(None, None)
    mp.put(map, 1, 2)
    # Test para el caso en que la llave existe
    assert mp.get(map, 1) == 2

    # Test para el caso en que la llave no existe
    assert mp.get(map, 2) is None

    # Test para el caso en que la tabla está vacía
    new_map = mp.new_map(5, 0.5, 7)
    assert mp.get(new_map, 1) is None


@handle_not_implemented
def test_remove():
    map = setup_tests(1, 0)
    mp.put(map, 1, 2)
    mp.put(map, 2, 3)
    mp.put(map, 3, 4)

    # Test para el caso en que la llave existe
    mp.remove(map, 1)
    assert map["size"] == 2
    assert not mp.contains(map, 1)
    assert me.get_key(lt.get_element(map["table"], 1)) == "__EMPTY__"
    assert me.get_value(lt.get_element(map["table"], 1)) == "__EMPTY__"

    # Test para el caso en que la llave no existe
    mp.remove(map, 1)
    assert map["size"] == 2

    # Test para el caso en que la tabla está vacía
    new_map = mp.new_map(5, 0.5, 7)
    mp.remove(new_map, 1)
    assert new_map["size"] == 0
    assert not mp.contains(new_map, 1)


@handle_not_implemented
def test_size():
    map = setup_tests(None, None)
    mp.put(map, 1, 2)
    mp.put(map, 2, 3)
    mp.put(map, 3, 4)

    assert mp.size(map) == 3

    new_map = mp.new_map(5, 0.5, 7)
    assert mp.size(new_map) == 0


@handle_not_implemented
def test_is_empty():
    map = setup_tests(None, None)
    mp.put(map, 1, 2)
    mp.put(map, 2, 3)
    mp.put(map, 3, 4)

    assert not mp.is_empty(map)

    new_map = mp.new_map(5, 0.5, 7)
    assert mp.is_empty(new_map)


@handle_not_implemented
def test_key_set():
    map = setup_tests(None, None)
    mp.put(map, 1, 2)
    mp.put(map, 2, 3)
    mp.put(map, 3, 4)

    key_set = mp.key_set(map)
    assert lt.size(key_set) == 3

    elements = key_set["elements"]

    assert 1 in elements
    assert 2 in elements
    assert 3 in elements

    mp.remove(map, 1)

    key_set = mp.key_set(map)
    assert lt.size(key_set) == 2

    elements = key_set["elements"]

    assert not 1 in elements
    assert 2 in elements
    assert 3 in elements

    new_map = mp.new_map(5, 0.5, 7)
    key_set = mp.key_set(new_map)
    assert lt.size(key_set) == 0


@handle_not_implemented
def test_value_set():
    map = setup_tests(None, None)
    mp.put(map, 1, 2)
    mp.put(map, 2, 3)
    mp.put(map, 3, 4)

    value_set = mp.value_set(map)
    assert lt.size(value_set) == 3

    elements = value_set["elements"]

    assert 2 in elements
    assert 3 in elements
    assert 4 in elements

    mp.remove(map, 1)

    value_set = mp.value_set(map)
    assert lt.size(value_set) == 2

    elements = value_set["elements"]

    assert not 2 in elements
    assert 3 in elements
    assert 4 in elements

    new_map = mp.new_map(5, 0.5, 7)
    value_set = mp.value_set(new_map)
    assert lt.size(value_set) == 0


@handle_not_implemented
def test_find_slot():
    map = setup_tests(1, 0)
    mp.put(map, 1, 2)
    mp.put(map, 2, 3)
    mp.put(map, 3, 4)

    # Test para el caso en que la llave existe

    ocupied_1, pos_1 = mp.find_slot(map, 1, mf.hash_value(map, 1))
    assert pos_1 == 1
    assert ocupied_1 == True

    ocupied_2, pos_2 = mp.find_slot(map, 2, mf.hash_value(map, 2))
    assert pos_2 == 2
    assert ocupied_2 == True

    ocupied_3, pos_3 = mp.find_slot(map, 3, mf.hash_value(map, 3))
    assert pos_3 == 3
    assert ocupied_3 == True

    # Test para el caso en que la llave no existe y retorna la posición donde debería ir la llave

    ocupied_4, pos_4 = mp.find_slot(map, 8, mf.hash_value(map, 8))
    assert pos_4 == 4
    assert ocupied_4 == False

    # Test para el caso en que la llave fue eliminada y retorna la posición donde debería ir la llave

    mp.remove(map, 2)
    ocupied_5, pos_5 = mp.find_slot(map, 8, mf.hash_value(map, 8))
    assert pos_5 == 2
    assert ocupied_5 == False


@handle_not_implemented
def test_is_available():
    map = setup_tests(1, 0)
    mp.put(map, 1, 2)
    mp.put(map, 2, 3)
    mp.put(map, 3, 4)

    assert mp.is_available(map["table"], mf.hash_value(map, 1)) == False
    assert mp.is_available(map["table"], mf.hash_value(map, 5)) == True
    assert mp.is_available(map["table"], mf.hash_value(map, 7)) == True

    # Test para el caso en que se elimina una llave y se verifica si la posición está disponible

    mp.remove(map, 2)
    assert mp.is_available(map["table"], mf.hash_value(map, 9)) == True


@handle_not_implemented
def test_rehash():
    map = mp.new_map(5, 0.5, 7)
    for i in range(5):
        mp.put(map, i, i)

    map = mp.rehash(map)

    assert mp.size(map) == 5
    assert map["capacity"] == 23

    for i in range(5):
        assert mp.contains(map, i)
