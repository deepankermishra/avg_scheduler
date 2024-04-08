from impl.consts import Status

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

        # Job status
        self.status = Status.PENDING


    def __str__(self):
        return f'Job: {self.id}, {self.num_workers}, {self.num_files}, {self.status}'

    def run(self):
        raise NotImplementedError