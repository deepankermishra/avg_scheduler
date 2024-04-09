import uuid
from impl.consts import Status

class Job:
    def __init__(self, name, num_workers, num_files, cardinality, input_dir, output_dir=None):
        # Job metadata
        self.id = uuid.uuid4()
        self.name = name

        # Job parameters
        self.num_workers = num_workers or 1
        self.num_files = num_files
        self.cardinality = cardinality

        # Assuming all the files in a given directory represet a given level
        # of the operation hierarchy.
        self.input_dir = input_dir
        self.output_dir = output_dir

        # Job status
        self.status = Status.PENDING


    def __str__(self):
        return f'Job: {self.name}, {self.num_workers}, {self.num_files}, {self.status}'

    def run(self):
        raise NotImplementedError