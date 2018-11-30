from condu import Condu, WorkflowTaskDef, DecisionTaskDef, WorkflowDef
from sample.samples.SampleTaskDefinitions import sample_task_definitions


def main():
    cw = Condu('http://localhost:8080/api')
    # creating the definitions of the tasks
    cw.create_tasks(sample_task_definitions())

    # rand, addition and multiplication tasks
    wt_rand = WorkflowTaskDef('sample_heads_or_tails')

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
    cw.create_workflow(sample_workflow)


if __name__ == '__main__':
    main()
