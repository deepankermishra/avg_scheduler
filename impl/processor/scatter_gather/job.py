import os
from concurrent.futures import as_completed

from impl.processor.job import Job
from impl.processor.scatter_gather.task import Task
from impl.utils import get_csv_file_paths
from impl.consts import Operation, Status, BATCH_SIZE, global_thread_pool

# A job is a collection of tasks that need to be executed.
# Each job has a set of files that need to be processed.
# The files are divided into batches and put on a task queue.
class ScatterGatherJob(Job):

    def __init__(self, name, num_workers, num_files, cardinality, input_dir, output_dir=None):
        super().__init__(name, num_workers, num_files, cardinality, input_dir, output_dir)
        self.level = 0
        self.is_last_op = False

    def run(self):
        input_dir = self.input_dir
        execution_level = self.level
        total_files = self.num_files
        print(f'Job {self.id} started')
        while 1:
            # SCATTER PHASE #
            # Read all the file paths from the files_dir.
            # This is a metadata operation. Does not actually load the files.
            file_paths = get_csv_file_paths(input_dir)
            if len(file_paths) == 0:
                print(f'No files found to be processed {input_dir}')
                return

            # Create an output directory for the job.
            output_dir = f'{input_dir}/output_{execution_level}'
            os.makedirs(output_dir, exist_ok=True)

            # Divide the files into batches and assign to workers
            batch_size = BATCH_SIZE
            print(f'Batch size: {batch_size}, File paths: {file_paths}')
            file_path_batches = [file_paths[i:i+batch_size] for i in range(0, len(file_paths), batch_size)]
            
            futures = []
            # Create a thread pool with the specified maximum number of workers
                # Divide work.
            for idx, file_path_batch in enumerate(file_path_batches):
                # Choose operation based on the remaining batches.
                operation = Operation.AVG if self.is_last_op else Operation.SUM
                print(f'Creating task for batch {idx} with operation {operation}')
                task = Task(self.id, execution_level, file_path_batch, output_dir,
                            total_files, operation=operation)

                future = global_thread_pool.submit(task.run)
                futures.append(future)
                print(f'Task {idx} added to the queue')
                
            # GATHER PHASE #
            # Wait for all the tasks to complete.
            # TODO: add retry and error handling if a task fails.
            for future in as_completed(futures):
                future.result()

            if self.is_last_op:
                break

            # This can be done as after a gather phase we have all the results from previous level.
            # Execute next level of the operation hierarchy.
            execution_level = execution_level + 1
            num_batches = len(file_path_batches)
            input_dir = output_dir
            
            # Last batch.
            self.is_last_op = num_batches == 1
            print(f'Execute the next level: {execution_level}, files: {num_batches}')
            
        self.status = Status.COMPLETE
        print(f'Job {self.id} completed')
        return
