from condu import Condu


def simple_task(task):
    task.status = 'COMPLETED'
    task.outputData = {'NETFLIX': 'RULES!'}
    task.append_to_logs('worked nice')


def main():
    print('Starting Sample worker')
    cw = Condu('http://localhost:8080/api')
    cw.start_workflow('sub_flow_1')
    cw.put_task('task_5', simple_task)
    cw.put_task('task_6', simple_task)
    # This runs the tasks forever in <processes> processes, blocking call
    cw.start_tasks(polling_interval=0.1, processes=5)
    # This executes a task one time, non-blocking call
    cw.poll_and_execute_task('task_5', simple_task)
    cw.poll_and_execute_task('task_6', simple_task)


if __name__ == '__main__':
    main()
