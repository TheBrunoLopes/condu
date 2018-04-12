from condu import Condu
from condu import WorkflowDef, ForkTaskDef, WorkflowTaskDef, JoinTaskDef
from sample.samples.SampleTaskDefinitions import sample_task_definitions


def main():
    cw = Condu('http://localhost:8080/api')
    # creating the definitions of each task
    cw.create_tasks(sample_task_definitions())

    # Defining the first task for the workflow ( fork_task addition )
    wt_add = WorkflowTaskDef('sample_task_addition')
    wt_add.inputParameters = {'foo': "${workflow.input.foo}", 'bar': "${workflow.input.bar}"}

    wt_mult = WorkflowTaskDef('sample_task_multiplication')
    wt_mult.inputParameters = {'foo': '${workflow.input.foo}', 'bar': '${workflow.input.bar}'}

    # Define the workflow task objects

    wt_div = WorkflowTaskDef('sample_task_division')
    wt_div.inputParameters = {'add_res': '${sample_task_addition.output.result}',
                              'mult_res': '${sample_task_multiplication.output.result}'}

    wt_fork = ForkTaskDef('fork_addition_and_multiplication')
    wt_fork.forkTasks = [[wt_add], [wt_mult]]

    wt_join = JoinTaskDef('join_addition_and_multiplication')

    wt_join.joinOn = ['fork_addition_and_multiplication', 'join_addition_and_multiplication']

    # Creating the definition for the workflow
    sample_workflow = WorkflowDef('sample_condu3_workflow_fork')
    sample_workflow.description = 'Calculate ((foo + bar) / (foo * bar)) in parallel'
    sample_workflow.inputParameters = ['foo', 'bar']
    sample_workflow.version = 1
    # "${sample_task_division.output.result}" is equal to wt_div.get_path('result')
    sample_workflow.outputParameters = {'result': wt_div.get_path('result')}
    sample_workflow.tasks = [wt_fork, wt_join, wt_div]
    cw.create_workflow(sample_workflow)


if __name__ == '__main__':
    main()
