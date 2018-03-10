from condu import Condu
from condu import WorkflowDef, WorkflowTaskDef
from sample.samples.SampleTaskDefinitions import sample_task_definitions


def sample_workflow_def(name: str, version: int, cw):
    # defining the tasks
    cw.create_tasks(sample_task_definitions())
    # Creating the definition for the workflow
    sample_workflow = WorkflowDef(name)
    sample_workflow.description = ' This workflow will make the the calculation of ((foo + bar) / (foo * bar)) '
    sample_workflow.inputParameters = ['foo', 'bar']
    sample_workflow.version = version
    sample_workflow.outputParameters = {'result': "${sample_task_division.output.result}"}
    # Defining the first task for the workflow ( addition )
    wt_add = WorkflowTaskDef('sample_task_addition', 'sample_task_addition', 'addition_task', {})
    wt_add.inputParameters = {'foo': "${workflow.input.foo}", 'bar': "${workflow.input.bar}"}

    wt_mult = WorkflowTaskDef('sample_task_multiplication', 'sample_task_multiplication', 'multiplication task', {})
    wt_mult.inputParameters = {'foo': '${workflow.input.foo}', 'bar': '${workflow.input.bar}'}

    wt_div = WorkflowTaskDef('sample_task_division', 'sample_task_division', 'division task', {})
    wt_div.inputParameters = {'add_res': '${sample_task_addition.output.result}',
                              'mult_res': '${sample_task_multiplication.output.result}'}

    sample_workflow.tasks = [wt_add, wt_mult, wt_div]
    cw.create_workflow(sample_workflow)


if __name__ == '__main__':
    condu = Condu('http://localhost:8080/api')
    sample_workflow_def('sample_condu3_workflow', 1, condu)
