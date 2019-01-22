import unittest.mock as mock
import pytest
from pytest_mock import mocker
import responses
from condu import condu
from multiprocessing import Pipe


def dummy_exec_fx(task):
    pass


@responses.activate
def test_task_acked_and_executed(mocker):
    conductor_server_uri = "http://localhost:8080/api"
    worker_id = "dummy"
    condu_client = condu.Condu(conductor_server_uri, hostname=worker_id)

    fake_task_name = "task_0"
    fake_task_id = "6c08ce48-e942-414b-8f75-a9af502d8c53"

    # mock response from request done inside tested method
    responses.add(responses.GET,
                  'http://localhost:8080/api/tasks/poll/{}?workerid={}'.format(
                      fake_task_name, worker_id),
                  json={
                      "taskId": fake_task_id
                  }, status=200, content_type="application/json")
    responses.add(responses.POST, 'http://localhost:8080/api/tasks/{}/ack'.format(fake_task_id),
                  json=True, status=200, content_type="application/json")

    ch_a, ch_b = Pipe()

    # mock objects called by function under test but not under test
    mocker.patch.object(condu_client, "_Condu__execute_task")
    condu_client._Condu__execute_task.return_value = None
    condu_client.poll_and_execute_task(fake_task_name, dummy_exec_fx, ch_b)
    assert condu_client._Condu__execute_task.call_count == 1


@responses.activate
def test_task_not_acked_thus_not_executed(mocker):
    conductor_server_uri = "http://localhost:8080/api"
    worker_id = "dummy"
    condu_client = condu.Condu(conductor_server_uri, hostname=worker_id)

    fake_task_name = "task_0"
    fake_task_id = "6c08ce48-e942-414b-8f75-a9af502d8c53"

    # mock response from request done inside tested method
    responses.add(responses.GET,
                  'http://localhost:8080/api/tasks/poll/{}?workerid={}'.format(
                      fake_task_name, worker_id),
                  json={
                      "taskId": fake_task_id
                  }, status=200, content_type="application/json")
    responses.add(responses.POST, 'http://localhost:8080/api/tasks/{}/ack'.format(fake_task_id),
                  json=False, status=200, content_type="application/json")

    ch_a, ch_b = Pipe()

    # mock objects called by function under test but not under test
    mocker.patch.object(condu_client, "_Condu__execute_task")
    condu_client._Condu__execute_task.return_value = None
    condu_client.poll_and_execute_task(fake_task_name, dummy_exec_fx, ch_b)
    assert condu_client._Condu__execute_task.call_count == 0


@responses.activate
def test_task_not_polled_thus_not_executed(mocker):
    conductor_server_uri = "http://localhost:8080/api"
    worker_id = "dummy"
    condu_client = condu.Condu(conductor_server_uri, hostname=worker_id)

    fake_task_name = "task_0"
    fake_task_id = "6c08ce48-e942-414b-8f75-a9af502d8c53"

    # mock response from request done inside tested method
    responses.add(responses.GET,
                  'http://localhost:8080/api/tasks/poll/{}?workerid={}'.format(
                      fake_task_name, worker_id),
                  json=None, status=204, content_type="application/json")

    ch_a, ch_b = Pipe()

    # mock objects called by function under test but not under test
    mocker.patch.object(condu_client, "_Condu__execute_task")
    condu_client._Condu__execute_task.return_value = None
    condu_client.poll_and_execute_task(fake_task_name, dummy_exec_fx, ch_b)
    assert condu_client._Condu__execute_task.call_count == 0

