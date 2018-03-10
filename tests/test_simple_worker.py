import signal

from condu import Condu


def cleanup(tlist):
    # function called when unblocking start_tasks
    print("This function is called for each proccess")


def simple_task(task):
    task.status = 'COMPLETED'
    task.outputData = {'testing': 'this'}
    task.append_to_logs('logging information')


def test_starting():
    cw = Condu('http://localhost:8080/api')
    cw.start_workflow('sub_flow_1')
    cw.put_task('task_5', simple_task)
    cw.put_task('task_6', simple_task)
    # This line starts the tasks with 1 process for each task. When we do CTRL + C (SIGINT)
    # we run cleanup function for each process and unblock start_tasks
    cw.start_tasks(polling_interval=0.1, processes=1, term_handler=cleanup)


if __name__ == '__main__':
    test_starting()
