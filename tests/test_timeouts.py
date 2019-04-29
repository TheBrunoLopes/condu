from condu import condu
from time import sleep
import responses
import socket
import multiprocessing as mp

fake_conductor_uri = "http://localhost:8080/api"
fake_task_blueprint = {
   "taskType": "tmp_task",
   "status": "SCHEDULED",
   "inputData": {
      "foo": "bar"
   },
   "referenceTaskName": "tmp_task2",
   "retryCount": 0,
   "seq": 2,
   "pollCount": 0,
   "taskDefName": "tmp_task",
   "scheduledTime": 1556531815953,
   "startTime": 0,
   "endTime": 0,
   "updateTime": 1556531815961,
   "startDelayInSeconds": 0,
   "retried": False,
   "executed": False,
   "callbackFromWorker": True,
   "responseTimeoutSeconds": 3600,
   "workflowInstanceId": "83e930cb-4891-40f8-b8e5-8e7ef1b616d5",
   "workflowType": "5cc1cf44d14d34000b498a4d",
   "taskId": "9eef76e7-b3f4-4c68-86e4-7df63a192898",
   "callbackAfterSeconds": 0,
   "workflowTask": {
      "name": "tmp_task",
      "taskReferenceName": "tmp_task2",
      "description": "tmp_task",
      "inputParameters": {
         "foo": "bar"
      },
      "type": "SIMPLE",
      "startDelay": 0,
      "optional": False,
      "taskDefinition": {
         "createTime": 1556132886211,
         "name": "tmp_task",
         "description": "tmp_task",
         "retryCount": 2,
         "timeoutSeconds": 0,
         "inputKeys": [
            "foo"
         ],
         "outputKeys": [

         ],
         "timeoutPolicy": "TIME_OUT_WF",
         "retryLogic": "FIXED",
         "retryDelaySeconds": 0,
         "responseTimeoutSeconds": 3600,
         "rateLimitPerFrequency": 0,
         "rateLimitFrequencyInSeconds": 1
      }
   },
   "rateLimitPerFrequency": 0,
   "rateLimitFrequencyInSeconds": 0,
   "taskStatus": "SCHEDULED",
   "queueWaitTime": 0,
   "taskDefinition": {
      "present": True
   },
   "logs": []
}


def tmp_task_deadlock(task):
    while True:
        pass


def tmp_task(task):
    sleep(20)
    task.status = "COMPLETED"


def call_start_tasks_with_timeout(conductor_client, timeout):
    conductor_client.start_tasks(processes=1, polling_interval=1, timeout=timeout)


@responses.activate
def test_timeout_10(mocker):
    # mocks response to pollForTask
    responses.add(method="GET",
                  url=fake_conductor_uri + "/tasks/poll/tmp_task" + "?workerid=" + socket.gethostname(),
                  status=200,
                  json=fake_task_blueprint,
                  content_type="application/json")
    # mocks response to ackTask
    responses.add(method="POST",
                  url=fake_conductor_uri + "/tasks/9eef76e7-b3f4-4c68-86e4-7df63a192898/ack" + "?workerid=" + socket.gethostname(),
                  status=200,
                  json=True,
                  content_type="application/json")
    # mock response to updateTask
    responses.add(method="POST",
                  url=fake_conductor_uri + "/tasks/",
                  status=200,
                  content_type="text/plain")

    conductor_client = condu.Condu(fake_conductor_uri)
    conductor_client.put_task("tmp_task", tmp_task)

    child_process = mp.Process(target=call_start_tasks_with_timeout, args=(conductor_client, 10,))
    child_process.start()
    child_process.join(60)
    child_process.terminate()
    # TODO: Make asserts. IF the logs present FAILED tasks this test doesn't detected errors


@responses.activate
def test_timeout_30():
    # mocks response to pollForTask
    responses.add(method="GET",
                  url=fake_conductor_uri + "/tasks/poll/tmp_task" + "?workerid=" + socket.gethostname(),
                  status=200,
                  json=fake_task_blueprint,
                  content_type="application/json")
    # mocks response to ackTask
    responses.add(method="POST",
                  url=fake_conductor_uri + "/tasks/9eef76e7-b3f4-4c68-86e4-7df63a192898/ack" + "?workerid=" + socket.gethostname(),
                  status=200,
                  json=True,
                  content_type="application/json")
    # mock response to updateTask
    responses.add(method="POST",
                  url=fake_conductor_uri + "/tasks/",
                  status=200,
                  content_type="text/plain")

    conductor_client = condu.Condu(fake_conductor_uri)
    conductor_client.put_task("tmp_task", tmp_task)

    child_process = mp.Process(target=call_start_tasks_with_timeout, args=(conductor_client, 30,))
    child_process.start()
    child_process.join(60)
    child_process.terminate()
    # TODO: Make asserts. IF the logs present COMPLETED tasks this test doesn't detected errors


@responses.activate
def test_without_timeout():
    # mocks response to pollForTask
    responses.add(method="GET",
                  url=fake_conductor_uri + "/tasks/poll/tmp_task" + "?workerid=" + socket.gethostname(),
                  status=200,
                  json=fake_task_blueprint,
                  content_type="application/json")
    # mocks response to ackTask
    responses.add(method="POST",
                  url=fake_conductor_uri + "/tasks/9eef76e7-b3f4-4c68-86e4-7df63a192898/ack" + "?workerid=" + socket.gethostname(),
                  status=200,
                  json=True,
                  content_type="application/json")
    # mock response to updateTask
    responses.add(method="POST",
                  url=fake_conductor_uri + "/tasks/",
                  status=200,
                  content_type="text/plain")

    conductor_client = condu.Condu(fake_conductor_uri)
    conductor_client.put_task("tmp_task", tmp_task)

    child_process = mp.Process(target=call_start_tasks_with_timeout, args=(conductor_client, None,))
    child_process.start()
    child_process.join(60)
    child_process.terminate()
    # TODO: Make asserts. IF the logs present COMPLETED tasks this test doesn't detected errors
