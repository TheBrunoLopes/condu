import signal

from condu import WorkflowTaskDef, JoinTaskDef, ForkTaskDef, WorkflowDef, TaskDef, SubWorkflowTaskDef, \
    DynamicForkTaskDef


def svm_workflow(workflow_name='svm_model_workflow'):
    # INPUTS OF WORKFLOW
    # - split_condition(type), attribute_names (type), attributes_to_ignore (type), class_label(type)
    # ---------------------------- SPLIT DATA SET -----------------------------------------------------------
    wt_split = WorkflowTaskDef('split_dataset')
    wt_split.inputParameters['dataset_uri'] = '${workflow.input.dataset_uri}'
    wt_split.inputParameters['split_condition'] = '${workflow.input.split_condition}'
    # ------ ATTRIBUTE SELECTION -----------
    wt_r_ras = WorkflowTaskDef('relieff_ranker_attribute_selection')
    wt_r_ras.inputParameters['dataset_uri'] = '${split_dataset.output.train_uri}'
    wt_r_ras.inputParameters['class_label'] = '${workflow.input.class_label}'
    wt_r_ras.inputParameters['num_instances_to_sample'] = '${workflow.input.num_instances_to_sample}'
    wt_r_ras.inputParameters['seed'] = '${workflow.input.seed}'
    wt_r_ras.inputParameters['num_neighbours'] = '${workflow.input.num_neighbours}'
    wt_r_ras.inputParameters['weight_by_distance'] = '${workflow.input.weight_by_distance}'
    wt_r_ras.inputParameters['sigma'] = '${workflow.input.sigma}'
    wt_r_ras.inputParameters['threshold'] = '${workflow.input.threshold}'
    wt_r_ras.inputParameters['num_attr_to_select'] = '${workflow.input.num_attr_to_select}'
    # ------ ATTRIBUTE ENCODING TRAIN -----------
    wt_haeta = WorkflowTaskDef('one_hot_attribute_encoding_train')
    wt_haeta.inputParameters['dataset_uri'] = '${relieff_ranker_attribute_selection.output.dataset_uri}'
    wt_haeta.inputParameters['attributes_to_encode'] = '${workflow.input.attributes_to_encode}'
    wt_haeta.inputParameters['attributes_to_ignore'] = '${workflow.input.attributes_to_ignore}'
    # ------ RAW ATTRIBUTE SELECTION ------------
    wt_ras = WorkflowTaskDef('raw_attribute_selection')
    wt_ras.inputParameters['dataset_uri'] = '${split_dataset.output.test_uri}'
    wt_ras.inputParameters['attributes'] = '${relieff_ranker_attribute_selection.output.selected_columns}'
    # ------ SCALING TRAIN ----------------------
    wt_fsta = WorkflowTaskDef('feature_scaling_train')
    wt_fsta.inputParameters['dataset_uri'] = '${one_hot_attribute_encoding_train.output.dataset_uri}'
    wt_fsta.inputParameters['attributes_to_scale'] = '${workflow.input.attributes_to_scale}'
    wt_fsta.inputParameters['mini'] = '${workflow.input.mini}'
    wt_fsta.inputParameters['maxi'] = '${workflow.input.maxi}'
    wt_fsta.inputParameters['attributes_to_ignore'] = '${workflow.input.attributes_to_ignore}'
    # ------ ATTRIBUTE ENCODING TEST-----------
    wt_haete = WorkflowTaskDef('one_hot_attribute_encoding_test')
    wt_haete.inputParameters['dataset_uri'] = '${raw_attribute_selection.output.dataset_uri}'
    wt_haete.inputParameters['train_cols'] = '${one_hot_attribute_encoding_train.output.dataset_columns}'
    wt_haete.inputParameters['attributes_to_ignore'] = '${workflow.input.attributes_to_ignore}'
    wt_haete.inputParameters['attributes_to_encode'] = '${workflow.input.attributes_to_encode}'
    # ------ SCALING TEST ---------------------
    wt_fste = WorkflowTaskDef('feature_scaling_test')
    wt_fste.inputParameters['dataset_uri'] = '${one_hot_attribute_encoding_test.output.dataset_uri}'
    wt_fste.inputParameters['scaler_uri'] = '${feature_scaling_train.output.scaler_uri}'
    wt_fste.inputParameters['attributes_to_scale'] = '${workflow.input.attributes_to_scale}'
    wt_fste.inputParameters['attributes_to_ignore'] = '${workflow.input.attributes_to_ignore}'
    # ------ SVM MODEL CREATION ---------------
    wt_smc = WorkflowTaskDef('svm_model_creation')
    wt_smc.inputParameters['dataset_uri'] = '${feature_scaling_train.output.dataset_uri}'
    wt_smc.inputParameters['class_label'] = '${workflow.input.class_label}'
    wt_smc.inputParameters['C'] = '${workflow.input.C}'
    wt_smc.inputParameters['kernel'] = '${workflow.input.kernel}'
    # ------ SVM MODEL TEST -------------------
    wt_smt = WorkflowTaskDef('svm_model_test')
    wt_smt.inputParameters['dataset_uri'] = '${feature_scaling_test.output.dataset_uri}'
    wt_smt.inputParameters['class_label'] = '${workflow.input.class_label}'
    wt_smt.inputParameters['model_uri'] = '${svm_model_creation.output.model_uri}'

    # ----- WORKFLOW DEFINITION ---------------
    # Workflow definition
    svm_model_workflow = WorkflowDef(workflow_name)
    svm_model_workflow.description = 'This workflow will take a dataset, and return a model with its respective ' \
                                     'performance '
    svm_model_workflow.inputParameters = ['dataset_uri', 'class_label', 'attributes_to_scale', 'attributes_to_encode',
                                          'attributes_to_ignore', 'split_condition', 'num_instances_to_sample', 'seed',
                                          'num_neighbours', 'weight_by_distance', 'sigma', 'threshold',
                                          'num_attr_to_select', 'mini', 'maxi', 'C', 'kernel', 'performance_metrics']
    svm_model_workflow.version = 1
    # Split task
    svm_model_workflow.tasks.append(wt_split)
    # Relieff ranked attribute selection task
    svm_model_workflow.tasks.append(wt_r_ras)
    # We now a fork of with 2 parallel branches
    wt_1_hot_fork = ForkTaskDef('1_hot_fork', '1_hot_fork', '1_hot_fork', {})
    #     Tasks one_hot_attribute_encoding_train, feature_scaling_train and raw_attribute_selection
    wt_1_hot_fork.forkTasks = [[wt_haeta, wt_fsta], [wt_ras]]
    svm_model_workflow.tasks.append(wt_1_hot_fork)
    # This join only waits for two tasks
    wt_1_hot_join = JoinTaskDef('1_hot_join')
    wt_1_hot_join.joinOn = ['one_hot_attribute_encoding_train', 'raw_attribute_selection']
    svm_model_workflow.tasks.append(wt_1_hot_join)
    # Task one_hot_attribute_encoding_test
    svm_model_workflow.tasks.append(wt_haete)
    # Join for the feature_scaling_train
    wt_2_scaling_train_join = JoinTaskDef('2_scaling_train_join')
    wt_2_scaling_train_join.joinOn = ['feature_scaling_train']
    svm_model_workflow.tasks.append(wt_2_scaling_train_join)
    # Another fork for the final tasks of svm_model_creation and feature_scaling_test
    wt_3_build_and_test_scaling_fork = ForkTaskDef('3_build_and_test_scaling_fork')
    wt_3_build_and_test_scaling_fork.forkTasks = [[wt_smc], [wt_fste]]
    svm_model_workflow.append_task(wt_3_build_and_test_scaling_fork)
    # Last join for these two tasks
    wt_4_build_and_test_scaling_join = JoinTaskDef('4_build_and_test_scaling_join')
    wt_4_build_and_test_scaling_join.joinOn = ['svm_model_creation', 'feature_scaling_test']
    svm_model_workflow.append_task(wt_4_build_and_test_scaling_join)
    # Last task is svm_model_test
    svm_model_workflow.append_task(wt_smt)
    return svm_model_workflow


def svm_model_workflow_with_eval():
    svm_model_workflow = svm_workflow('svm_workflow_with_classifier')
    # ----- CLASSIFIER MODEL EVAL ------
    wt_cme = WorkflowTaskDef('classifier_model_eval')
    wt_cme.inputParameters['targets'] = '${svm_model_test.output.targets}'
    wt_cme.inputParameters['predicted'] = '${svm_model_test.output.predicted}'
    wt_cme.inputParameters['performance_metrics'] = '${workflow.input.performance_metrics}'
    svm_model_workflow.tasks.append(wt_cme)
    return svm_model_workflow


def task_definitions_for_svm():
    task_list = []
    # --------- SPLIT DATASET ----------------
    split_dataset = TaskDef('split_dataset')
    split_dataset.description = 'Some decription'
    split_dataset.retryCount = 2
    split_dataset.inputKeys = ['dataset_uri', 'split_condition', 'stratified', 'randomize', 'seed']
    split_dataset.outputKeys = ['train_uri', 'test_uri']
    split_dataset.responseTimeoutSeconds = 0
    task_list.append(split_dataset)
    # ---------------------------------------
    # --------- RELIEFF RANKER --------------
    relieff_ranker = TaskDef('relieff_ranker_attribute_selection')
    split_dataset.description = 'Some decription'
    relieff_ranker.retryCount = 2
    relieff_ranker.inputKeys = ['dataset_uri', 'class_label', 'num_instances_to_sample', 'seed', 'num_neighbours',
                                'weight_by_distance', 'sigma', 'threshold', 'num_attr_to_select']
    relieff_ranker.outputKeys = ['dataset_uri', 'selected_columns', 'dummy_results']
    relieff_ranker.responseTimeoutSeconds = 0
    task_list.append(relieff_ranker)
    # ----------------------------------------
    # --------- ATTRIBUTE SELECTION ----------
    raw_attribute = TaskDef('raw_attribute_selection')
    raw_attribute.description = 'Attribute selection'
    raw_attribute.retryCount = 2
    raw_attribute.inputKeys = ['dataset_uri', 'attributes']
    raw_attribute.outputKeys = ['dataset_uri']
    raw_attribute.responseTimeoutSeconds = 0
    task_list.append(raw_attribute)
    # ---------------------------------------
    # --------- ATTRIBUTE ENCODING TRAIN ----
    attribute_encoding_train = TaskDef('one_hot_attribute_encoding_train')
    attribute_encoding_train.description = 'one_hot_attribute_encoding_train'
    attribute_encoding_train.retryCount = 2
    attribute_encoding_train.inputKeys = ['dataset_uri', 'attributes_to_encode', 'attributes_to_ignore']
    attribute_encoding_train.outputKeys = ['dataset_uri', 'dataset_columns']
    attribute_encoding_train.responseTimeoutSeconds = 0
    task_list.append(attribute_encoding_train)
    # ---------------------------------------
    # --------- ATTRIBUTE ENCODING TEST -----
    attribute_encoding_test = TaskDef('one_hot_attribute_encoding_test')
    attribute_encoding_test.description = 'one_hot_attribute_encoding_test'
    attribute_encoding_test.retryCount = 2
    attribute_encoding_test.inputKeys = ['dataset_uri', 'attributes_to_encode', 'train_cols', 'attributes_to_ignore']
    attribute_encoding_test.outputKeys = ['dataset_uri']
    attribute_encoding_test.responseTimeoutSeconds = 0
    task_list.append(attribute_encoding_test)
    # ---------------------------------------
    # --------- SCALING TRAIN ---------------
    feature_scaling_train = TaskDef('feature_scaling_train')
    feature_scaling_train.description = 'feature scaling train'
    feature_scaling_train.retryCount = 2
    feature_scaling_train.inputKeys = ['dataset_uri', 'mini', 'maxi', 'attributes_to_scale', 'attributes_to_ignore']
    feature_scaling_train.outputKeys = ['dataset_uri', 'scaler_uri', 'scaled_attributes']
    feature_scaling_train.responseTimeoutSeconds = 0
    task_list.append(feature_scaling_train)
    # ---------------------------------------
    # --------- SCALING TEST ----------------
    feature_scaling_test = TaskDef('feature_scaling_test')
    feature_scaling_test.description = 'feature scaling test'
    feature_scaling_test.retryCount = 2
    feature_scaling_test.inputKeys = ['dataset_uri', 'scaler_uri', 'attributes_to_scale', 'attributes_to_ignore']
    feature_scaling_test.outputKeys = ['dataset_uri', 'scaled_attributes']
    feature_scaling_test.responseTimeoutSeconds = 0
    task_list.append(feature_scaling_test)
    # ---------------------------------------
    # --------- SVM MODEL CREATION ----------
    svm_model_creation = TaskDef('svm_model_creation')
    svm_model_creation.description = 'ML model creation'
    svm_model_creation.retryCount = 2
    svm_model_creation.inputKeys = ['dataset_uri', 'class_label', 'C', 'kernel']
    svm_model_creation.outputKeys = ['model_uri']
    svm_model_creation.responseTimeoutSeconds = 0
    task_list.append(svm_model_creation)
    # ---------------------------------------
    # --------- SVM MODEL TEST --------------
    svm_model_test = TaskDef('svm_model_test')
    svm_model_test.description = 'ML model test'
    svm_model_test.retryCount = 2
    svm_model_test.inputKeys = ['dataset_uri', 'class_label', 'model_uri']
    svm_model_test.outputKeys = ['confusion_matrix', 'targets', 'predicted']
    svm_model_test.responseTimeoutSeconds = 0
    task_list.append(svm_model_test)
    # ---------------------------------------
    # Cross validation task definitions
    # ---------------------------------------
    # -------- CLASSIFIER MODEL EVAL --------
    class_model_eval = TaskDef('classifier_model_eval')
    class_model_eval.description = 'classifier_model_eval'
    class_model_eval.retryCount = 2
    class_model_eval.inputKeys = ['targets', 'predicted', 'performance_metrics']
    class_model_eval.outputKeys = ['model_performance']
    class_model_eval.responseTimeoutSeconds = 0
    task_list.append(class_model_eval)
    # ---------------------------------------
    # -------- MULTIPLE MODEL EVALS ---------
    mult_model_eval = TaskDef('multiple_model_evals')
    mult_model_eval.description = 'multiple_model_evals'
    mult_model_eval.retryCount = 2
    mult_model_eval.inputKeys = ['model_performances']
    mult_model_eval.outputKeys = ['global_performance']
    mult_model_eval.responseTimeoutSeconds = 0
    task_list.append(mult_model_eval)
    # ---------------------------------------
    # -------- CV FOLDS DEFINITION ----------
    cv_folds_defini = TaskDef('cv_folds_definition')
    cv_folds_defini.description = 'cv_folds_definition'
    cv_folds_defini.retryCount = 2
    cv_folds_defini.inputKeys = ['dataset_uri', 'num_folds', 'stratified', 'randomize', 'seed', 'class_label']
    cv_folds_defini.outputKeys = ['fold_definitions', 'dataset_uri']
    cv_folds_defini.responseTimeoutSeconds = 0
    task_list.append(cv_folds_defini)
    # ---------------------------------------
    return task_list


# ----- Util for the sub workflows creation ---------
def multiple_cross_workflow_util(task, num_cross=3, outputs: list = None):
    task.outputData['dynamicTaskNames'] = []
    task.outputData['dynamicTaskInputs'] = {}
    if outputs is None:
        outputs = [{'dataset_uri': None,
                    'split_condition': None
                    } for _ in range(num_cross)]
    for i in range(num_cross):
        sub_w = spawn_sub_workflow_task_util(i, outputs[i])
        task.outputData['dynamicTaskNames'].append(sub_w[0].get_dict())
        task.outputData['dynamicTaskInputs'].update(sub_w[1])


def spawn_sub_workflow_task_util(i, output):
    sub_workflow_name = 'cross_val_workflow_{0}'.format(i)
    workflow_inputs = ['class_label', 'attributes_to_scale', 'attributes_to_encode',
                       'attributes_to_ignore', 'num_instances_to_sample', 'seed',
                       'num_neighbours', 'weight_by_distance', 'sigma', 'threshold',
                       'num_attr_to_select', 'mini', 'maxi', 'C', 'kernel', 'performance_metrics']

    wt_sub_t = SubWorkflowTaskDef(sub_workflow_name)
    wt_sub_t.sub_workflow_name = 'svm_workflow_with_classifier'

    wt_sub_t.inputParameters = {workflow_input: '${workflow.input.' + workflow_input + '}' for
                                workflow_input in workflow_inputs}
    wt_sub_t.inputParameters.update(output)
    dynamic_input = {sub_workflow_name: output}
    return wt_sub_t, dynamic_input

# ---------------------------------------------------


# creation of a workflow with a dynamic fork that spawns multiple subworkflows
def cross_workflow():
    wt_cv_folds_def = WorkflowTaskDef('cv_folds_definition')
    wt_cv_folds_def.inputParameters['dataset_uri'] = '${workflow.input.dataset_uri}'
    wt_cv_folds_def.inputParameters['num_folds'] = '${workflow.input.num_folds}'
    wt_cv_folds_def.inputParameters['stratified'] = '${workflow.input.stratified}'
    wt_cv_folds_def.inputParameters['randomize'] = '${workflow.input.randomize}'
    wt_cv_folds_def.inputParameters['seed'] = '${workflow.input.seed}'
    wt_cv_folds_def.inputParameters['class_label'] = '${workflow.input.class_label}'

    wt_dynamic = DynamicForkTaskDef('dynamic_cross')
    wt_dynamic.inputParameters['dynamicTaskNames'] = '${cv_folds_definition.output.dynamicTaskNames}'
    wt_dynamic.inputParameters['dynamicTaskInputs'] = '${cv_folds_definition.output.dynamicTaskInputs}'
    wt_dynamic.dynamicForkTasksParam = 'dynamicTaskNames'
    wt_dynamic.dynamicForkTasksInputParamName = 'dynamicTaskInputs'

    wt_join = JoinTaskDef('1_join', '1_join', '1_join', {})

    wt_multiple_models = WorkflowTaskDef('multiple_model_evals')
    wt_multiple_models.inputParameters['model_performances'] = '${1_join.output}'

    cross_svm_model_workflow = WorkflowDef('cross_validation_svm_workflow')
    cross_svm_model_workflow.description = 'cross_validation_svm_workflow'
    cross_svm_model_workflow.inputParameters = ['dataset_uri', 'class_label', 'attributes_to_scale',
                                                'attributes_to_encode', 'attributes_to_ignore', 'split_condition',
                                                'num_instances_to_sample', 'seed', 'num_neighbours',
                                                'weight_by_distance', 'sigma', 'threshold', 'num_attr_to_select',
                                                'mini', 'maxi', 'C', 'kernel', 'num_folds', 'stratified', 'randomize',
                                                'seed', 'performance_metrics']
    cross_svm_model_workflow.version = 1
    cross_svm_model_workflow.tasks.append(wt_cv_folds_def)
    cross_svm_model_workflow.tasks.append(wt_dynamic)
    cross_svm_model_workflow.tasks.append(wt_join)
    cross_svm_model_workflow.tasks.append(wt_multiple_models)
    return cross_svm_model_workflow


def simple_task(task):
    task.status = 'COMPLETED'
    task.outputData = {'BLACK BOX': 'TESTING'}
    task.append_to_logs('SOME LOG')


def cv_folds_task(task):
    num_folds = task.inputData.get('num_folds')
    num_folds = num_folds if num_folds is not None else 3
    outputs = []
    for i in range(num_folds):
        outputs.append({'dataset_uri': 'cv_folds the data set or it gets the hose', 'split_condition': [i]})
    multiple_cross_workflow_util(task, num_cross=num_folds, outputs=outputs)
    task.status = 'COMPLETED'

