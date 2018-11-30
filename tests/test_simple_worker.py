from condu import Condu

simple_task_name = 'task_5'
another_simple_task_name = 'task_6'
simple_workflow_name = 'sub_flow_1'
endpoint = 'http://localhost:8080/api'


def task_worker(task):
    assert task.status == 'IN_PROGRESS'
    assert task.taskDefName.startswith('task_')
    task.status = 'COMPLETED'


# start a workflow and check if it's running
def test_start_workflow():
    cw = Condu(endpoint)
    w_id = cw.start_workflow(simple_workflow_name)
    w_list = cw.workflow_client.getRunningWorkflows(simple_workflow_name)
    assert w_id in w_list


# poll for a task in the previous workflow
def test_simple_task_execution():
    cw = Condu(endpoint)
    cw.poll_and_execute_task(simple_task_name, task_worker)


# Poll for another task in that workflow that is only scheduled if simple_task was completed
def test_another_simple_task():
    cw = Condu(endpoint)
    cw.poll_and_execute_task(another_simple_task_name, task_worker)


if __name__ == '__main__':
    test_simple_task_execution()
