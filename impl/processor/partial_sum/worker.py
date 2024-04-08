import uuid
import pandas as pd

from time import sleep
from random import randint

from impl.processor.partial_sum.db import TaskMetadata, WorkerMetadata, task_queue, workers_state, jobs_state
from impl.consts import Status, global_thread_pool, MAX_WORKERS

#  A worker can process any unit of work from the task queue.
class Worker:
    def __init__(self):
        self.id = uuid.uuid4()  # Should be generated in a DB.

    def run(self):
        print(f'Worker {self.id} is running')
        while 1:
            # Pick task from task_queue.
            unit_work = task_queue.get()
            print(f'Worker {self.id} is processing task {unit_work.job_id} with input files {unit_work.input_path}')

            # Lookup job.
            job = jobs_state[unit_work.job_id]

            cur_state = self.get_worker_state(unit_work.job_id)
            print(f'Worker {self.id} has cur state {cur_state}')
            if not cur_state:
                print('Init worker current state')
                cur_state = WorkerMetadata(self.id, unit_work.job_id, None, 0)

            print(f'Worker {self.id} has cur state {cur_state} and cur job state {job}')

            # Update partial sum for the job id.
            if unit_work.task_type == 'partial_sum':
                df = pd.read_csv(unit_work.input_path, header=None)
                print(f'Worker {self.id} is processing partial sum task and read file {unit_work.input_path} with df {df}')
                
                if cur_state.partial_sum_df is None:
                    cur_state.partial_sum_df = df
                else:
                    cur_state.partial_sum_df += df
                cur_state.files_processed += 1
                print(f'Worker {self.id} partial sum {cur_state.partial_sum_df}')
                self.update_worker_state(cur_state)

            # This must be a completion task.
            else: 
                files_processed = self.get_files_processed(unit_work.job_id)
                if files_processed == job.num_files:
                    print(f'Job {unit_work.job_id} is completed')
                    # Write the result to a file.
                    output_file = f'{job.output_dir}/result.csv'
                    avg_df = self.get_average_of_full_sum(unit_work.job_id)
                    print(f'Writing output to {output_file} with avg df {avg_df}')
                    avg_df.to_csv(output_file, index=False, header=None)
                    job.status = Status.COMPLETE
                    self.update_job_state(job)
                    print(f'Output written to {output_file}')
                else:
                    print(f'Job {unit_work.job_id} is not completed yet. {cur_state.files_processed}/{job.total_files} files processed, requeueing the task')
                    # Requeue completion task.
                    task_queue.put(TaskMetadata(self.id, None, task_type='completion', status=Status.PENDING), True)

            # Indicates last get operation on queue is complete.
            print(f'Worker {self.id} is done processing task {unit_work.job_id}')
            task_queue.task_done()
            sleep(randint(1, 2))

    def get_worker_state(self, job_id) -> WorkerMetadata:
        return workers_state.get((self.id,job_id), None)

    def update_worker_state(self, cur_state) -> bool:
        workers_state[(self.id, cur_state.job_id)] = cur_state
        return True

    def update_job_state(self, job) -> bool:
        jobs_state[job.id] = job
        return True
    
    # This operation access state of all workers responsible for a job.
    # TODO: this can be improved.
    def get_average_of_full_sum(self, job_id) -> pd.DataFrame:
        total_sum = None
        for work_state_key in workers_state.keys():
            _, w_job_id = work_state_key
            if w_job_id != job_id:
                continue
            state = workers_state[work_state_key]
            if total_sum is None:
                total_sum = state.partial_sum_df
            else:
                total_sum += state.partial_sum_df
        return total_sum / jobs_state[job_id].num_files

    # This operation access state of all workers responsible for a job.
    # TODO: this can be improved.
    def get_files_processed(self, job_id) -> bool:
        total_files_processed = 0
        for work_state_key in workers_state.keys():
            worker_id, w_job_id = work_state_key
            if w_job_id != job_id:
                continue
            state = workers_state[work_state_key]
            total_files_processed += state.files_processed
        return total_files_processed

def setup_workers() -> None:
    # Create a thread pool with the specified maximum number of workers
    # Start the workers.
    workers = [Worker() for _ in range(MAX_WORKERS)]
    for worker in workers:
        global_thread_pool.submit(worker.run)