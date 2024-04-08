from impl.processor.base_job import Job
from impl.processor.partial_sum.db import TaskMetadata, task_queue, jobs_state
from impl.utils import get_csv_file_paths

# Files are simply queued in as work units.
# A fixed number of workers are listening for events on this queue.
# They pick up work as soon as it is available and update their partial sum.
class PartialSums(Job):
    def __init__(self, id, num_workers, num_files, cardinality, files_dir):
        super().__init__(id, num_workers, num_files, cardinality, files_dir)

    def partial_sum(self):
        jobs_state[self.id] = self
        file_paths = get_csv_file_paths(self.input_dir)
        if len(file_paths) == 0:
            print(f'No files found to be processed {self.input_dir}')
            return
        # Put work for each file on the task queue.
        for file_path in file_paths:
            task_queue.put(TaskMetadata(self.id, file_path), True) # Block till a free slot is available in the queue.
        
        print(f'Added {len(file_paths)} tasks to the queue')

        # Add a completion marker task.
        task_queue.put(TaskMetadata(self.id, None, task_type='completion'), True) # Block till a free slot is available in the queue.

