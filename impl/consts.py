import os
from enum import Enum

# Enums for job status
class Status(Enum):
    PENDING = 0
    RUNNING = 1
    COMPLETE = 2
    FAILED = 3


class Operation(Enum):
    SUM = 0
    AVG = 1

# Number of files a worker can process in a batch.
# TODO: Can be made a function of number_worker input from user.
BATCH_SIZE = os.getenv('BATCH_SIZE', 2)

JOB_DISPATCH_ALGO = os.getenv('JOB_DISPATCH_ALGO', 'SCATTER_GATHER')
