# This Dockerfile should be used only to run test_worker_process_kill
FROM python:3

WORKDIR /app

COPY . /app

RUN pip install requests

COPY tests/worker_process_killed_by_os/test_worker_process_kill.py .

CMD ["python", "test_worker_process_kill.py"]