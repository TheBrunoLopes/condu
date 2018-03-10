import random

from condu.condu import Condu


def addition_function(task):
    result = float(task.inputData.get('foo')) + float(task.inputData.get('bar'))
    task.outputData = {'result': result}
    task.status = 'COMPLETED'


def multiplication_function(task):
    result = float(task.inputData.get('foo')) * float(task.inputData.get('bar'))
    task.outputData = {'result': result}
    task.status = 'COMPLETED'


def division_function(task):
    result = float(task.inputData.get('add_res')) / float(task.inputData.get('mult_res'))
    task.outputData = {'result': result}
    task.status = 'COMPLETED'


def heads_or_tails_function(task):
    task.outputData = {'result': 'heads' if random.randint(0, 1) == 0 else 'tails'}
    task.status = 'COMPLETED'


def main():
    print('Starting Sample to execute a workflow')
    cw = Condu('http://localhost:8080/api')
    cw.start_workflow('heads_or_tails_workflow', {'foo': 2, 'bar': 5})
    cw.put_task('sample_task_addition', addition_function)
    cw.put_task('sample_task_multiplication', multiplication_function)
    cw.put_task('sample_task_division', division_function)
    cw.put_task('sample_heads_or_tails', heads_or_tails_function)
    cw.start_tasks()


if __name__ == '__main__':
    main()
