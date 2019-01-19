# This test should be performed in a container with memory limit < 200 MB to see the worker
# being killed by the OS due to Out Of Memory and created again by condu
# In the main directory do: docker build -t test-worker-process-kill . && docker run -m 100m --network=host test-worker-process-kill

from condu.condu import Condu
from condu.objects import TaskDef
from condu.objects import WorkflowDef
from condu.objects import WorkflowTaskDef
import signal
import logging

MBYTE = 1024*1024
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def test_task(task):
    test_str = ""
    i = 0
    while i < 400:
        test_str += " " * MBYTE
        i += 1
        logger.info("test_str with {} MB.".format(i))
    task.status = 'COMPLETED'


def register_task():
    task = TaskDef('test_task')
    task.description = 'Test task'
    task.retryCount = 3
    task.inputKeys = []
    task.outputKeys = []
    task.responseTimeoutSeconds = 120
    return task


def define_and_start_workflow(cClient):
    def def_workflow():
        wfDef = WorkflowDef("test_wf")
        wfDef.tasks = [WorkflowTaskDef("test_task")]
        return wfDef

    cClient.create_workflow(def_workflow())
    cClient.start_workflow("test_wf")


if __name__ == '__main__':
    cClient = Condu('http://localhost:8080/api', signum=signal.SIGTERM)
    cClient.create_task(register_task())
    define_and_start_workflow(cClient)
    cClient.put_task('test_task', test_task)
    cClient.start_tasks()
