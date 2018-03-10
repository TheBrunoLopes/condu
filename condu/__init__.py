import sys
from .condu import Condu
from .objects import WorkflowTaskDef, ForkTaskDef, MetaTask, DecisionTaskDef, DynamicForkTaskDef, DynamicTaskDef
from .objects import JoinTaskDef, SubWorkflowTaskDef, TaskDef, WorkflowDef

import logging


class SingleLevelFilter(logging.Filter):
    def __init__(self, passlevel, reject):
        super().__init__()
        self.passlevel = passlevel
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return record.levelno != self.passlevel
        else:
            return record.levelno == self.passlevel


# We could have also used logging.NullHandler() if we wanted no logs when the
# user doesn't specify any logger configuration
handler_stdout = logging.StreamHandler(sys.stdout)
# The SingleLevelFilter are configured in so that the error message doesn't go to stdout
handler_stdout.addFilter(SingleLevelFilter(logging.INFO, False))
handler_stderr = logging.StreamHandler(sys.stderr)
# Configuration done so that INFO goes to stdout and EVERYTHING else goes to stderr
handler_stderr.addFilter(SingleLevelFilter(logging.INFO, True))
formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%d/%m/%Y %I:%M:%S')
handler_stdout.setFormatter(formatter)
handler_stderr.setFormatter(formatter)
# We use __name__ so that it doesn't conflict with the user's loggers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler_stdout)
logger.addHandler(handler_stderr)

