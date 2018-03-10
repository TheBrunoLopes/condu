from condu import Condu
from condu import TaskDef


def sample_task_definitions():
    add_t = TaskDef('sample_task_addition')
    add_t.inputKeys = ['foo', 'bar']
    add_t.outputKeys = ['result']

    mult_t = TaskDef('sample_task_multiplication')
    mult_t.inputKeys = ['foo', 'bar']
    mult_t.outputKeys = ['result']

    div_t = TaskDef('sample_task_division')
    div_t.inputKeys = ['add_res', 'mult_res']
    div_t.outputKeys = ['result']

    rand_t = TaskDef('sample_heads_or_tails')
    rand_t.outputKeys = ['result']

    return [add_t, mult_t, div_t, rand_t]


if __name__ == '__main__':
    cw = Condu('http://localhost:8080/api')
    cw.create_tasks(sample_task_definitions())
