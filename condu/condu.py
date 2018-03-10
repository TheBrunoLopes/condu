import signal
import socket
import time
from threading import Event

from condu.objects import MetaTask, TaskDef, WorkflowDef
from condu.conductor import WFClientMgr
from multiprocessing import Process
import logging

logger = logging.getLogger(__name__)


class Condu(WFClientMgr):
    def __init__(self, server_url: str, hostname: str = socket.gethostname()):
        super().__init__(server_url)
        self.tasks = {}
        self.hostname = hostname
        self.shutdown_event = Event()
        # ----------------------
        # variables that gracefully allows to stop start_tasks()
        # handler supplied on start_tasks
        self.term_handler = lambda tasks: tasks
        # original handler
        self._signal_handler = None
        self._signum = signal.SIGINT
        # ----------------------

    # This function is called with _signum so that the blocking call start_tasks() can be unblocked
    def computer_says_stop(self, signum, frame):
        try:
            self.term_handler(self.tasks)
        except TypeError as terr:
            # It means the handler is not callable or that it doesn't take 1 required parameter
            # Producing a warning and stopping start_tasks anyway
            logger.error('TypeError, term_handler not called : {0}'.format(terr))
        except Exception as exc:
            # For the TypeError we could stop start_tasks
            # but for an unknown error we can't because term_handler() failed executing
            logger.error(exc)
            return
        # Restoring the original signal handler
        signal.signal(self._signum, self._signal_handler)
        self.shutdown_event.set()

    # ------------------ *************** ------------------
    # ------------------ Task Management ------------------

    def remove_task(self, task_name: str):
        self.tasks.pop(task_name)

    def put_task(self, task_name: str, func):
        self.tasks[task_name] = func

    def __signal_definitions(self, term_handler, term_signal):
        # sets the handler supplied
        self._signal_handler = signal.getsignal(self._signum)
        self._signum = term_signal if term_signal is not None else self._signum
        signal.signal(self._signum, self.computer_says_stop)
        if callable(term_handler):
            self.term_handler = term_handler

    def start_tasks(self, polling_interval: float = 0.1, processes: int = 1, term_handler: callable = None,
                    term_signal: int = None):

        """ Blocking method, creates <processes> child processes for each task; each process polls and executes the task in
        with at least the interval of <polling_interval> seconds. A signal can also be sent to gracefully unblock
        this method Processes were adopted instead of threads because of the python's GIL
        """
        self.__signal_definitions(term_handler, term_signal)
        for task in self.tasks:
            exec_function = self.tasks[task]
            for i in range(processes):
                process = Process(target=self.__start_task, args=(task, exec_function, polling_interval))
                process.daemon = True
                # https://docs.python.org/3.4/library/multiprocessing.html?highlight=process#multiprocessing.Process
                process.start()
        # This is a blocking call that blocks this thread (main one) until shutdown_event.set() is called
        self.shutdown_event.wait()

    def __start_task(self, task_name, exec_function, polling_interval):
        while 1:
            self.poll_and_execute_task(task_name, exec_function)
            time.sleep(polling_interval)

    def poll_and_execute_task(self, task_name, exec_function):
        # Insert the polling cycle here
        logger.debug('Polling for [' + task_name + '] as [' + self.hostname + ']')
        conductor_task = self.task_client.pollForTask(task_name, self.hostname)
        if conductor_task is not None:
            self.__execute_task(conductor_task, exec_function)

    def __execute_task(self, conductor_task, exec_function):
        """ Executes the exec_function and updates the task status in conductor """
        condu_task = MetaTask(conductor_task)
        try:
            exec_function(condu_task)
        except Exception as err:
            condu_task.status('FAILED')
            condu_task.append_to_logs(str(err))
            logger.error('Task set as FAILED', exc_info=True)
        self.task_client.updateTask(condu_task.get_dict())
        logger.info("[" + condu_task.status + "] [" + condu_task.referenceTaskName + "]-[" + condu_task.taskId + "]-[" +
                    condu_task.status + "] in the workflow - [" + condu_task.workflowInstanceId + "]")

    # ------------------ ******************* ------------------
    # ------------------ Workflow Management ------------------
    def start_workflow(self, workflow_name: str, input_data=None, version: int = None, correlation_id: str = None):
        if input_data is None:
            input_data = {}
        logger.info('starting workflow {0}'.format(workflow_name))
        return self.workflow_client.startWorkflow(workflow_name, input_data, version, correlation_id)

    def get_workflow(self, workflow_id):
        return self.workflow_client.getWorkflow(workflow_id)

    def terminate_workflow(self, workflow_id):
        return self.workflow_client.terminateWorkflow(workflow_id)

    def pause_workflow(self, workflow_id):
        self.workflow_client.pauseWorkflow(workflow_id)

    def resume_workflow(self, workflow_id):
        self.workflow_client.resumeWorkflow(workflow_id)

    # ------------------ **************** ------------------
    # ------------------ Task Definitions ------------------

    def create_task(self, task_def: TaskDef):
        task_def_list = [task_def.get_dict()]
        return self.metadata_client.registerTaskDefs(task_def_list)

    def create_tasks(self, task_def_list: list):
        return self.metadata_client.registerTaskDefs([task_def.get_dict() for task_def in task_def_list])

    # ------------------ ******************** ------------------
    # ------------------ Workflow Definitions ------------------
    def create_workflow(self, workflow_def: WorkflowDef):
        return self.metadata_client.updateWorkflowDefs([workflow_def.get_dict()])
