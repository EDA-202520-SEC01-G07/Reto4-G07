from DataStructures.List import single_linked_list as sl
def new_stack():
    my_stack = sl.new_list()
    return my_stack
def pop(my_stack):
    if sl.is_empty(my_stack):
        raise Exception("EmptyStructureError: stack is empty")
    else:
        elemento = sl.last_element(my_stack)
        sl.remove_last(my_stack)
        return elemento
def push(my_stack, elemento):
    sl.add_last(my_stack, elemento)
    return my_stack
def is_empty(my_stack):
    x = sl.is_empty(my_stack)
    return x
def top(my_stack):
    if not sl.is_empty(my_stack):
        return sl.last_element(my_stack)
    else:
        raise Exception("EmptyStructureError: stack is empty") 
def size(my_stack):
    return sl.size(my_stack)