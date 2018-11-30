from condu import Condu
from sample.samples.SampleWorflowDefinition import sample_workflow_def

sample_workflow_name = 'sample_workflow'
conductor_endpoint = 'http://localhost:8080/api'


def create_multiple_workflow_versions():
    cw = Condu(conductor_endpoint)
    for i in range(1, 11):
        sample_workflow_def(sample_workflow_name, i, cw)


def start_last_workflow_version():
    cw = Condu(conductor_endpoint)
    return cw.start_workflow(sample_workflow_name)


def get_last_workflow_version():
    cw = Condu(conductor_endpoint)
    return cw.metadata_client.getWorkflowDef(sample_workflow_name)


def get_running_workflows():
    cw = Condu(conductor_endpoint)
    return cw.workflow_client.getRunningWorkflows(sample_workflow_name)


# Create a workflow and check if it was created
def test_workflows_creation():
    create_multiple_workflow_versions()
    wf = get_last_workflow_version()
    assert wf['name'] == sample_workflow_name


# Start a workflow and see if it's in the list of running workflows
def test_running_worflows():
    w_id = start_last_workflow_version()
    assert type(w_id) is str
    w_list = get_running_workflows()
    assert w_id in w_list


# Check if when retrieving a workflow from conductor (without specifying version) we get the latest version
def test_workflow_versioning():
    wf = get_last_workflow_version()
    assert wf['version'] == 10


if __name__ == '__main__':
    test_workflows_creation()
