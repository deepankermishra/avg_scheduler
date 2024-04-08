# This file is used as an in-memory db.

import queue
import pandas as pd

from impl.consts import Status

# Thread safe multi-producer, multi-consumer queue.
task_queue = queue.Queue()

# In-memory state of the workers.
workers_state = {}

# In-memory state of the jobs.
jobs_state = {}


class TaskMetadata:
    def __init__(self, job_id, input_path, task_type= 'partial_sum'):
        self.job_id = job_id
        self.input_path = input_path
        self.task_type = task_type
        self.status = Status.PENDING


class WorkerMetadata:
    def __init__(self, worker_id, job_id):
        self.worker_id = worker_id
        self.job_id = job_id
        self.partial_sum_df = pd.DataFrame()
        self.files_processed = 0
