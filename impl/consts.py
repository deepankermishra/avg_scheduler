import os
from enum import Enum
from concurrent.futures import ThreadPoolExecutor

# Enums for job status
class Status(Enum):
    PENDING = 0
    RUNNING = 1
    COMPLETE = 2
    FAILED = 3

class Operation(Enum):
    SUM = 0
    AVG = 1

class JOB_ALGO(Enum):
    SCATTER_GATHER = 0
    PARTIAL_SUMS = 1
    ACCUMULATOR = 2


# Number of files a worker can process in a batch.
# TODO: Can be made a function of number_worker input from user.
BATCH_SIZE = os.getenv('BATCH_SIZE', 5)
MAX_WORKERS = os.getenv('MAX_WORKERS', 4)
WAIT_TIME_SEC = os.getenv('WAIT_TIME_SEC', 2)
MAX_WAIT_TIME_SEC = os.getenv('MAX_WAIT_TIME_SEC', 60)
EXECUTION_LOCATION = os.getenv('EXECUTION_LOCATION', './data')
JOB_DISPATCH_ALGO = os.getenv('JOB_DISPATCH_ALGO', JOB_ALGO.ACCUMULATOR)

global_thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
