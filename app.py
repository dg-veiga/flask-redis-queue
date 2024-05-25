import json
from datetime import datetime
from time import sleep

from flask import Flask, jsonify, request
from rq import Queue
from rq.job import Job
from rq.registry import FinishedJobRegistry, StartedJobRegistry

from worker import conn

TIME_FORMAT = "%m/%d/%Y, %H:%M:%S"

app = Flask(__name__)

q = Queue(connection=conn)

registry = StartedJobRegistry('default', connection=conn)
finished_registry = FinishedJobRegistry('default', connection=conn)


def operation(param: str = None):
    t_start = datetime.now()
    sleep(10)
    t_end = datetime.now()
    t_process = t_start - t_end
    result = {
        'param': param,
        'start': t_start.strftime(TIME_FORMAT),
        'end': t_end.strftime(TIME_FORMAT),
        'time_to_process': t_process.total_seconds(),
    }
    return json.dumps(result, indent=4)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/fetch/", methods=['GET'])
def fetch():
    if request.method == 'GET':
        job = q.enqueue_call(func=operation,  kwargs={
                             'param': '123'}, result_ttl=5000)
        job_id = job.get_id()
        print(job_id)
        return f"Fetch operated successfully. ID: {job_id}", 200


@app.route("/results/<job_key>", methods=['GET'])
def get_result(job_key):
    job = Job.fetch(job_key, connection=conn)
    if job.is_finished:
        return str(job.result), 200
    else:
        return "Nay!", 202


@app.route("/results/", methods=['GET'])
def get_results():
    running_job_ids = registry.get_job_ids()
    expired_job_ids = registry.get_expired_job_ids()
    finished_job_ids = finished_registry.get_job_ids()

    jobs = {
        'running_job_ids': running_job_ids,
        'expired_job_ids': expired_job_ids,
        'finished_job_ids': finished_job_ids
    }
    return jsonify(jobs)
