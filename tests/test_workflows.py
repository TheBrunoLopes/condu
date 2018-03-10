from condu import Condu
from sample.samples.SampleWorflowDefinition import sample_workflow_def

sample_workflow_name = 'sample_workflow'
conductor_endpoint = 'http://localhost:8080/api'


def test_multiple_workflow_versions():
    cw = Condu(conductor_endpoint)
    for i in range(1, 51):
        sample_workflow_def(sample_workflow_name, i, cw)


def start_last_workflow_version():
    cw = Condu(conductor_endpoint)
    cw.start_workflow(sample_workflow_name)


def get_last_workflow_version():
    cw = Condu(conductor_endpoint)
    workflow = cw.metadata_client.getWorkflowDef(sample_workflow_name)


def get_running_workflows_last_version():
    cw = Condu(conductor_endpoint)
    workflows = cw.workflow_client.getRunningWorkflows(sample_workflow_name)


if __name__ == '__main__':
    test_multiple_workflow_versions()
    start_last_workflow_version()
    get_last_workflow_version()
    condu = Condu(conductor_endpoint)
    condu.start_workflow(sample_workflow_name, version=5)
    get_running_workflows_last_version()
