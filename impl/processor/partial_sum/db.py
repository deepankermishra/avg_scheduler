# This file is used as an in-memory db.

import queue
from dataclasses import dataclass
from uuid import UUID
from impl.consts import Status
import pandas as pd

# Thread safe multi-producer, multi-consumer queue.
task_queue = queue.Queue()

# In-memory state of the workers.
workers_state = {}

# In-memory state of the jobs.
jobs_state = {}

@dataclass
class TaskMetadata:
    job_id: str
    input_path: str
    task_type: str
    status: Status

@dataclass
class WorkerMetadata:
    worker_id: UUID
    job_id: str
    partial_sum_df: pd.DataFrame
    files_processed: int
