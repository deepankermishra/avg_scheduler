import os
from enum import Enum
from concurrent.futures import ThreadPoolExecutor


# Number of files a worker can process in a batch.
# TODO: Can be made a function of number_worker input from user.
BATCH_SIZE = os.getenv('BATCH_SIZE', 2)
MAX_WORKERS = os.getenv('MAX_WORKERS', 4)

EXECUTION_LOCATION = os.getenv('EXECUTION_LOCATION', './data')

class JOB_ALGO(Enum):
    SCATTER_GATHER = 0
    PARTIAL_SUMS = 1

JOB_DISPATCH_ALGO = os.getenv('JOB_DISPATCH_ALGO', JOB_ALGO.PARTIAL_SUMS)

global_thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# Enums for job status
class Status(Enum):
    PENDING = 0
    RUNNING = 1
    COMPLETE = 2
    FAILED = 3


class Operation(Enum):
    SUM = 0
    AVG = 1
