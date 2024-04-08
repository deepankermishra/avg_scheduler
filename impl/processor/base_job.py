
from impl.consts import Status, JOB_DISPATCH_ALGO, JOB_ALGO
from impl.processor.partial_sum.worker import setup_workers


class Job:
    def __init__(self, id, num_workers, num_files, cardinality, input_dir, output_dir=None):
        # Job metadata
        self.id = id

        # Job parameters
        self.num_workers = num_workers or 1
        self.num_files = num_files
        self.cardinality = cardinality

        # Assuming all the files in a given directory represet a given level
        # of the operation hierarchy.
        self.input_dir = input_dir
        self.output_dir = output_dir
        # Level of the operation hierarchy
        self.level = 0
        # Job status
        self.status = Status.PENDING
        # Info of file batches.
        self.is_last_op = False

        self.workerIds = set() # This set will be thread safe due to GIL.


    def __str__(self):
        return f'Job: {self.id}, {self.num_workers}, {self.num_files}, {self.status}'
    
    def scatter_gather(self):
        raise NotImplementedError("Must override in subclass")

    def partial_sum(self):
        raise NotImplementedError("Must override in subclass")

    def run(self):
        self.status = Status.RUNNING
        print(f'Job {self.id} is running')

        if JOB_DISPATCH_ALGO == JOB_ALGO.SCATTER_GATHER:
            return self.scatter_gather()
        elif JOB_DISPATCH_ALGO == JOB_ALGO.PARTIAL_SUMS:
            setup_workers()
            return self.partial_sum()
        else:
            raise ValueError(f'Invalid job dispatch algorithm {JOB_DISPATCH_ALGO}')
