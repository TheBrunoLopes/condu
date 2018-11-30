# --------------  ****************** -------------
# --------------  Management Objects -------------


class MetaTask(object):
    def __init__(self, task: dict):
        # int
        self.callbackAfterSeconds = task.get('callbackAfterSeconds')
        # bool
        self.callbackFromWorker = task.get('callbackFromWorker')
        # int
        self.endTime = task.get('endTime')
        # int
        self.pollCount = task.get('pollCount')
        # int
        self.queueWaitTime = task.get('queueWaitTime')
        # str
        self.referenceTaskName = task.get('referenceTaskName')
        #  int
        self.responseTimeoutSeconds = task.get('responseTimeoutSeconds')
        # bool
        self.retried = task.get('retried')
        # int
        self.scheduledTime = task.get('scheduledTime')
        # int
        self.seq = task.get('seq')
        # int
        self.startDelayInSeconds = task.get('startDelayInSeconds')
        # int
        self.startTime = task.get('startTime')
        # str - Can be 'IN_PROGRESS','FAILED', 'COMPLETED' and 'CANCELED'
        self.status = task.get('status')
        # str
        self.taskDefName = task.get('taskDefName')
        # str
        self.taskId = task.get('taskId')
        # str - can have the same values as status
        self.taskStatus = task.get('taskStatus')
        # int
        self.updateTime = task.get('updateTime')
        # str
        self.workerId = task.get('workerId')
        # str
        self.workflowInstanceId = task.get('workflowInstanceId')
        # dict
        self.workflowTask = task.get('workflowTask')
        # str
        self.taskType = task.get('taskType')
        # int
        self.retryCount = task.get('retryCount')
        # ---
        self.inputData = task.get('inputData')
        self.outputData = {}
        self.logs = []

    def append_to_logs(self, log: str):
        self.logs.append(log)

    # Returns the items that are not none
    def get_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


# --------------- ******************* ----------------------
# --------------- Definition objects -----------------------


class TaskDef(object):
    def __init__(self, name) -> None:
        # str | Task name
        self.name = name
        # int | No. of retries to attempt when a task is marked as failure
        self.retryCount = None
        # str | 'FIXED' : Reschedule the task after the retryDelaySeconds
        #       'EXPONENTIAL'_BACKOFF : reschedule after retryDelaySeconds  * attempNo
        self.retryLogic = None
        # int | Time in milliseconds, after which the task is marked as TIMED_OUT if not
        # completed after transiting to IN_PROGRESS status
        self.timeoutSeconds = 0  # No timeouts if set to 0
        # str | Task's timeout policy
        #       'RETRY' : Retries the task again
        #       'TIME_OUT_WF' : Workflow is marked as TIMED_OUT and terminated
        #       'ALERT_ONLY' : Registers a counter (task_timeout)
        self.timeoutPolicy = None
        # int | if greater than 0, the task is rescheduled if not updated with a status after this time.
        self.responseTimeoutSeconds = None
        # list of str| Set of keys of task's output. Used for documenting task's output
        self.outputKeys = []
        # list of str| Set of keys of task's input. Used for documenting task's input
        self.inputKeys = []
        # The time it takes to retry a task after it FAILED (I guess, it's not documented in conductor)
        self.retryDelaySeconds = 0
        # str | Not documented but they have a description property
        self.description = None

    # Returns the items that are not none
    def get_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


class WorkflowDef(object):
    def __init__(self, name: str) -> None:
        # str  | Name of the workflow
        self.name = name
        # str  | Descriptive name of the workflow
        self.description = None
        # int  | Numeric field used to identify the version of the schema
        self.version = 1
        # list | An array of workflow task definitions
        self.tasks = []
        # dict | JSON template used to generate the output of the workflow
        # If not specified, the output is defined as the output of the last executed task
        self.outputParameters = None
        # list | List of input parameters. Used for documenting the required inputs to workflow
        self.inputParameters = None
        # If this variable is not set to 2 things don't work  -_-
        self.schemaVersion = 2

    def append_task(self, task):
        self.tasks.append(task)

    # Returns the items that are not none
    def get_dict(self) -> dict:
        _dict = {k: v for k, v in self.__dict__.items() if v is not None}
        _dict['tasks'] = [task.get_dict() for task in self.tasks]
        return _dict

    def get_path(self, path, put='input') -> str:
        return '${workflow.'+put+'.'+path+'}'


class WorkflowTaskDef(object):
    def __init__(self, name: str, task_reference: str = None, description: str = None, input_parameters: dict = None,
                 optional: bool = False, c3_type: str = 'SIMPLE') -> None:
        # str  | Name of the task. MUST be registered as a task type with Conductor before starting workflow
        self.name = name
        # str  | Alias used to refer the task within the workflow. MUST be unique.
        self.taskReferenceName = task_reference if task_reference is not None else name
        # str  | Type of task. SIMPLE for tasks executed by remote workers, or one of the system task types
        self.type = c3_type
        # str  | Description of the task
        self.description = description if description is not None else name
        # dict | Defines the inputs given to the task
        # In order to take values from the workflow or another tasks
        # we use ${SOURCE.input/output.JSONPath}
        self.inputParameters = input_parameters if input_parameters is not None else {}
        # bool | When set to true - workflow continues even if the task fails.
        # The status of the task is reflected as COMPLETED_WITH_ERRORS
        self.optional = optional

    # Returns the items that are not none
    def get_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    def get_path(self, path, put='output') -> str:
        return '${'+self.taskReferenceName+'.'+put+'.'+path+'}'

# --------------- **************** ----------------------
# ---------------   SYSTEM TASKS   ----------------------

class ForkTaskDef(WorkflowTaskDef):

    def __init__(self, name: str, task_reference: str = None, description: str = None, input_parameters: dict = None,
                 optional: bool = False, c3_type: str = 'FORK_JOIN') -> None:
        super().__init__(name, task_reference, description, input_parameters, optional, c3_type=c3_type)
        # list | A list of list of tasks -> [[], [], [], ...]
        self.forkTasks = []

    # Returns the items that are not none
    def get_dict(self) -> dict:
        c_tasks = []
        for stream in self.forkTasks:
            f_stream = []
            for task in stream:
                f_stream.append(task.get_dict())
            c_tasks.append(f_stream)
        _dict = super().get_dict()
        _dict['forkTasks'] = c_tasks
        return _dict


class JoinTaskDef(WorkflowTaskDef):
    def __init__(self, name: str, task_reference: str = None, description: str = None, input_parameters: dict = None,
                 optional: bool = False, c3_type: str = 'JOIN') -> None:
        super().__init__(name, task_reference, description, input_parameters, optional, c3_type=c3_type)
        self.joinOn = []

    def get_dict(self) -> dict:
        _dict = super().get_dict()
        _dict['joinOn'] = self.joinOn
        return _dict


class DecisionTaskDef(WorkflowTaskDef):
    def __init__(self, name: str, task_reference: str = None, description: str = None, input_parameters: dict = None,
                 optional: bool = False, c3_type: str = 'DECISION') -> None:
        super().__init__(name, task_reference, description, input_parameters, optional, c3_type=c3_type)
        # str  | Name of the parameter in task input whose value will be used as a switch.
        self.caseValueParam = None
        # dict | Dictionary where key is possible values of caseValueParam with value being list of tasks to be executed
        self.decisionCases = {}
        # list | List of tasks to be executed when no matching value if found in decision case (default condition)
        self.defaultCase = []

    def get_dict(self) -> dict:
        _dict = super().get_dict()
        _dict['caseValueParam'] = self.caseValueParam
        for key, value in self.decisionCases.items():
            self.decisionCases[key] = [task.get_dict() for task in value]
        _dict['decisionCases'] = self.decisionCases
        _dict['defaultCase'] = [task.get_dict() for task in self.defaultCase]
        return _dict


class SubWorkflowTaskDef(WorkflowTaskDef):
    def __init__(self, name: str, task_reference: str = None, description: str = None, input_parameters: dict = None,
                 optional: bool = False, c3_type: str = 'SUB_WORKFLOW') -> None:
        super().__init__(name, task_reference, description, input_parameters, optional, c3_type)
        # str | Name of the workflow to be executed
        self.sub_workflow_name = None
        # str | Version of the workflow
        self.sub_workflow_version = None

    def get_dict(self) -> dict:
        _dict = super().get_dict()
        _dict['subWorkflowParam'] = {
            'name': self.sub_workflow_name,
            'version': self.sub_workflow_version
        }
        return _dict


class DynamicTaskDef(WorkflowTaskDef):
    def __init__(self, name: str, task_reference: str = None, description: str = None, input_parameters: dict = None,
                 optional: bool = False, c3_type: str = 'DYNAMIC') -> None:
        super().__init__(name, task_reference, description, input_parameters, optional, c3_type)
        # str | dynamicTaskNameParam	Name of the parameter from the task input whose value is used to schedule the
        # task. e.g. if the value of the parameter is ABC, the next task scheduled is of type 'ABC'
        self.dynamicTaskNameParam = None

    def get_dict(self) -> dict:
        _dict = super().get_dict()
        _dict['dynamicTaskNameParam'] = self.dynamicTaskNameParam
        return _dict


class DynamicForkTaskDef(WorkflowTaskDef):
    def __init__(self, name: str, task_reference: str = None, description: str = None, input_parameters: dict = None,
                 optional: bool = False, c3_type: str = 'FORK_JOIN_DYNAMIC') -> None:
        super().__init__(name, task_reference, description, input_parameters, optional, c3_type)
        # str | Name of the parameter that contains list of workflow task configuration to be executed in parallel
        self.dynamicForkTasksParam = None
        # str | Name of the parameter whose value should be a map with key as forked task's reference name and value
        # as input the forked task
        self.dynamicForkTasksInputParamName = None

    def get_dict(self) -> dict:
        _dict = super().get_dict()
        _dict['dynamicForkTasksParam'] = self.dynamicForkTasksParam
        _dict['dynamicForkTasksInputParamName'] = self.dynamicForkTasksInputParamName
        return _dict
