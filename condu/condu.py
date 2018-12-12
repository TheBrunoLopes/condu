import signal
import socket
import time
from threading import Event
import sys

from condu.objects import MetaTask, TaskDef, WorkflowDef
from condu.conductor import WFClientMgr
import multiprocessing as mp
import logging

logger = logging.getLogger(__name__)


class Condu(WFClientMgr):
    def __init__(self, server_url: str, hostname: str = socket.gethostname(), signum=signal.SIGINT):
        super().__init__(server_url)
        self.tasks = {}
        self.hostname = hostname
        self._shutdown_event = Event()
        # ----------------------
        # variables that gracefully allows to stop start_tasks()
        # handler supplied on start_tasks
        self._child_signal_handler = self.__terminate_task
        # original handler
        self._signum = signum
        self._conductor_task = None
        self._processes_list = None
        # ----------------------

    # used to terminate current _condu_task if the process has any
    def __terminate_task(self):
        if self._conductor_task is not None:
            condu_task = MetaTask(self._conductor_task)
            condu_task.status = 'FAILED'
            condu_task.append_to_logs("Task FAILED due to worker shutdown.")
            self.task_client.updateTask(condu_task.get_dict())
            logger.error('Task set as FAILED due to worker shutdown.', exc_info=True)
        sys.exit(0)

    # This function is called with _signum so that the blocking call start_tasks() can be unblocked
    def __parent_signal_handler(self, signum, frame):
        try:
            for process in self._processes_list:
                process.terminate()
                print("Child Process terminated")
        except TypeError as terr:
            # It means the handler is not callable or that it doesn't take 1 required parameter
            # Producing a warning and stopping start_tasks anyway
            logger.error('TypeError, term_handler not called : {0}'.format(terr))
        except Exception as exc:
            # For the TypeError we could stop start_tasks
            # but for an unknown error we can't because term_handler() failed executing
            logger.error(exc)
        finally:
            self._shutdown_event.set()
            sys.exit(0)

    # ------------------ *************** ------------------
    # ------------------ Task Management ------------------

    def remove_task(self, task_name: str):
        self.tasks.pop(task_name)

    def put_task(self, task_name: str, func):
        self.tasks[task_name] = func

    def __signal_definitions(self, child_signal_handler, signum):
        # sets the handler supplied
        self._signum = signum if signum is not None else self._signum
        if callable(child_signal_handler):
            self._child_signal_handler = child_signal_handler
        signal.signal(self._signum, self.__parent_signal_handler)

    def start_tasks(self, polling_interval: float = 0.2, processes: int = 1, child_signal_handler: callable = None,
                    signum: int = None):

        """ Blocking method, creates <processes> child processes for each task; each process polls and executes the task in
        with at least the interval of <polling_interval> seconds. A signal can also be sent to gracefully unblock
        this method Processes were adopted instead of threads because of the python's GIL
        """
        self._processes_list = []
        self.__signal_definitions(child_signal_handler, signum)
        mp.set_start_method("fork")
        for task in self.tasks:
            exec_function = self.tasks[task]
            for i in range(processes):
                process = mp.Process(target=self.__start_task, args=(task, exec_function, polling_interval))
                process.daemon = True
                # https://docs.python.org/3.4/library/multiprocessing.html?highlight=process#multiprocessing.Process
                process.start()
                self._processes_list.append(process)
        # This is a blocking call that blocks this thread (main one) until shutdown_event.set() is called
        self._shutdown_event.wait()

    def __start_task(self, task_name, exec_function, polling_interval):
        def child_process_shutdown(signum, frame):
            self._child_signal_handler()

        # Sets the handler for SIGTERM in the child process. SIGTERM can be triggered in parent process.
        signal.signal(signal.SIGTERM, child_process_shutdown)

        while 1:
            self.poll_and_execute_task(task_name, exec_function)
            time.sleep(polling_interval)

    def poll_and_execute_task(self, task_name, exec_function):
        # Insert the polling cycle here
        logger.debug('Polling for [' + task_name + '] as [' + self.hostname + ']')
        self._conductor_task = self.task_client.pollForTask(task_name, self.hostname)
        if self._conductor_task is not None:
            self.__execute_task(self._conductor_task, exec_function)

    def __execute_task(self, conductor_task, exec_function):
        """ Executes the exec_function and updates the task status in conductor """
        condu_task = MetaTask(conductor_task)
        try:
            exec_function(condu_task)
        except Exception as err:
            condu_task.status = 'FAILED'
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

    # ------------------ ******************* ------------------
    # ------------------    Workflow UTILS   ------------------
    def get_task_from_workflow(self, workflow, task_ref):
        if workflow is str:
            workflow = self.get_workflow(workflow)
        for task in workflow.get('tasks'):
            if task.get('referenceTaskName') == task_ref:
                if task['status'] == 'FAILED':
                    task['logs'] = self.task_client.get_task_logs(task['taskId'])
                return task
        return None

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
