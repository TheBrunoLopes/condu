import signal
from multiprocessing import Process

import time

from condu import Condu, TaskDef, WorkflowDef, JoinTaskDef, ForkTaskDef, WorkflowTaskDef, DecisionTaskDef, \
    DynamicForkTaskDef
from tests.metadata_testing.test_utils import task_definitions_for_svm, svm_workflow, svm_model_workflow_with_eval, \
    cross_workflow, simple_task, cv_folds_task

cw = Condu('http://localhost:8080/api')


def task_objects():
    add_t = TaskDef('sample_task_addition')
    add_t.inputKeys = ['foo', 'bar']
    add_t.outputKeys = ['result']

    mult_t = TaskDef('sample_task_multiplication')
    mult_t.inputKeys = ['foo', 'bar']
    mult_t.outputKeys = ['result']

    div_t = TaskDef('sample_task_division')
    div_t.inputKeys = ['add_res', 'mult_res']
    div_t.outputKeys = ['result']

    rand_t = TaskDef('sample_heads_or_tails')
    rand_t.outputKeys = ['result']

    return [add_t, mult_t, div_t, rand_t]


# tests the creation of tasks
def test_task_creation():
    resp = cw.create_tasks(task_objects())
    assert type(resp) is str


# tests the tasks previously created and with a fork and join tasks
def test_workflow_fork_join_creation():
    # Defining the first task for the workflow ( fork_task addition )
    wt_add = WorkflowTaskDef('sample_task_addition', 'sample_task_addition', 'addition_task', {})
    wt_add.inputParameters = {'foo': "${workflow.input.foo}", 'bar': "${workflow.input.bar}"}

    wt_mult = WorkflowTaskDef('sample_task_multiplication', 'sample_task_multiplication', 'multiplication task', {})
    wt_mult.inputParameters = {'foo': '${workflow.input.foo}', 'bar': '${workflow.input.bar}'}

    # Define the workflow task objects

    wt_div = WorkflowTaskDef('sample_task_division', 'sample_task_division', 'division task', {})
    wt_div.inputParameters = {'add_res': '${sample_task_addition.output.result}',
                              'mult_res': '${sample_task_multiplication.output.result}'}

    wt_fork = ForkTaskDef('fork_addition_and_multiplication', 'addition_and_multiplication',
                          'A simple but complex fork', {})
    wt_fork.forkTasks = [[wt_add], [wt_mult]]

    wt_join = JoinTaskDef('join_addition_and_multiplication', 'join_addition_and_multiplication',
                          'A simple but complex join', {})

    wt_join.joinOn = ['sample_task_addition', 'sample_task_multiplication']

    # Creating the definition for the workflow
    sample_workflow = WorkflowDef('sample_condu3_workflow_fork')
    sample_workflow.description = 'Calculate ((foo + bar) / (foo * bar)) in parallel'
    sample_workflow.inputParameters = ['foo', 'bar']
    sample_workflow.version = 1
    sample_workflow.outputParameters = {'result': "${sample_task_division.output.result}"}
    sample_workflow.tasks = [wt_fork, wt_join, wt_div]
    assert cw.create_workflow(sample_workflow).status_code == 204


# same as previous test but with decision task
def test_workflow_decision_creation():
    # rand, addition and multiplication tasks
    wt_rand = WorkflowTaskDef('sample_heads_or_tails', 'sample_heads_or_tails', 'This task generates randoms', {})

    wt_add = WorkflowTaskDef('sample_task_addition')
    wt_add.inputParameters = {'foo': "${workflow.input.foo}", 'bar': "${workflow.input.bar}"}

    wt_mult = WorkflowTaskDef('sample_task_multiplication')
    wt_mult.inputParameters = {'foo': '${workflow.input.foo}', 'bar': '${workflow.input.bar}'}
    # --------------------------------------------------------------------------------------------------
    # Decision task
    wt_decision = DecisionTaskDef('decision', description='Decides where to go depending on coin toss')
    wt_decision.inputParameters['coin'] = '${sample_heads_or_tails.output.result}'
    wt_decision.caseValueParam = 'coin'
    wt_decision.decisionCases['heads'] = [wt_add]
    wt_decision.decisionCases['tails'] = [wt_mult]

    # Workflow definition
    sample_workflow = WorkflowDef('heads_or_tails_workflow')
    sample_workflow.description = ' This workflow will toss a coin if heads it does ((foo + bar) else (foo * bar)) '
    sample_workflow.inputParameters = ['foo', 'bar']
    sample_workflow.version = 2
    sample_workflow.tasks = [wt_rand, wt_decision]
    assert cw.create_workflow(sample_workflow).status_code == 204


# Test the creation of extremely complex workflows defined in the context of a data science project
def test_create_svm_task_and_workflows():
    ct_resp = cw.create_tasks(task_definitions_for_svm())
    assert type(ct_resp) is str
    assert cw.create_workflow(svm_workflow()).status_code == 204
    assert cw.create_workflow(svm_model_workflow_with_eval()).status_code == 204
    assert cw.create_workflow(cross_workflow()).status_code == 204


def dynamic_worker():
    cw.put_task('split_dataset', simple_task)
    cw.put_task('cv_folds_definition', cv_folds_task)
    workflow_inputs = {
                       "split_condition": "I put pineapple on pizza",
                       "num_folds": 6
                       }
    w_id = cw.start_workflow('cross_validation_svm_workflow', workflow_inputs)
    assert type(w_id) is str
    cw.start_tasks(processes=2, polling_interval=0.3, term_signal=signal.SIGTERM, term_handler=lambda o: exit(0))


def test_dynamic_execution():
    p = Process(target=dynamic_worker)
    p.start()
    time.sleep(3)
    p.terminate()
    p.join()
    assert p.exitcode is None or p.exitcode == 0
    print(len(cw.workflow_client.getRunningWorkflows('svm_workflow_with_classifier')))
    assert len(cw.workflow_client.getRunningWorkflows('svm_workflow_with_classifier')) >= 6


if __name__ == '__main__':
    test_task_creation()
    test_workflow_fork_join_creation()
    test_dynamic_execution()
