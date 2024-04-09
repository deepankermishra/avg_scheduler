from impl.processor.job import Job
from impl.processor.partial_sum.db import TaskMetadata, task_queue, jobs_state
from impl.utils import get_csv_file_paths
from impl.consts import Status

# Files are simply queued in as work units.
# A fixed number of workers are listening for events on this queue.
# They pick up work as soon as it is available and update their partial sum.
class PartialSum(Job):
    def __init__(self, name, num_workers, num_files, cardinality, input_dir, output_dir=None):
        super().__init__(name, num_workers, num_files, cardinality, input_dir, output_dir)

    def run(self):
        self.status = Status.RUNNING
        jobs_state[self.id] = self
        file_paths = get_csv_file_paths(self.input_dir)
        if len(file_paths) == 0:
            print(f'No files found to be processed {self.input_dir}')
            return
        # Put work for each file on the task queue.
        for file_path in file_paths:
            print(f'Adding task for file {file_path}')
            task_queue.put(TaskMetadata(self.id, file_path, task_type='partial_sum', status=Status.PENDING), True) # Block till a free slot is available in the queue.
        
        print(f'Added {len(file_paths)} tasks to the queue')

        # Add a completion marker task.
        task_queue.put(TaskMetadata(self.id, None, task_type='completion', status=Status.PENDING), True) # Block till a free slot is available in the queue.
