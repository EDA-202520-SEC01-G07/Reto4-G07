from DataStructures.List import single_linked_list as lt

def new_queue():
    my_queue = lt.new_list()
    return my_queue

def enqueue(my_queue, element):
    lt.add_last(my_queue, element)
    return my_queue

def dequeue(my_queue):
    if is_empty(my_queue):
        raise Exception("EmptyStructureError: queue is empty")
    else:
        element = lt.remove_first(my_queue)
    return element
def is_empty(my_queue):
    x = lt.is_empty(my_queue)
    return x
def peek(my_queue):
    if not is_empty(my_queue):
        return lt.get_element(my_queue, 0)
    else:
        raise Exception("EmptyStructureError: queue is empty")
def size(my_queue):
    return lt.size(my_queue)