import uuid
import pandas as pd

from time import sleep
from random import randint

from impl.processor.partial_sum.db import WorkerMetadata, task_queue, workers_state, jobs_state
from impl.consts import global_thread_pool, MAX_WORKERS

#  A worker can process any unit of work from the task queue.
class Worker:
    def __init__(self):
        self.id = uuid.uuid4()  # Should be generated in a DB.

    def run(self):
        # pick task from task_queue
        unit_work = task_queue.get()
        cur_state = self.get_worker_state(unit_work.job_id)
        cur_job_state = jobs_state[unit_work.job_id]
        if not cur_state:
            cur_state = WorkerMetadata(self.id, unit_work.job_id)
            # Updates a shared set. Revisit this.
            jobs_state[unit_work.job_id].workers.add(self.id)

        # update partial sum for the job id
        if unit_work.task_type == 'partial_sum':
            df = pd.read_csv(unit_work.file_path, header=None)
            cur_state.partial_sum_df += df
            cur_state.files_processed += 1
            self.update_worker_state(cur_state)

        # this must be a completion task
        else: 
            files_processed = self.get_files_processed(unit_work.job_id)
            if files_processed  == cur_job_state.total_files:
                print(f'Job {unit_work.job_id} is completed')
                # write the result to a file
                output_file = f'{cur_job_state.output_dir}/result.csv'
                avg_df = self.get_average_of_full_sum(unit_work.job_id)
                avg_df.to_csv(output_file, index=False, header=None)
                print(f'Output written to {output_file}')
            else:
                print(f'Job {unit_work.job_id} is not completed yet. {cur_state.files_processed}/{cur_job_state.total_files} files processed, requeueing the task')
                # requeue
                task_queue.put(unit_work)

        # Indicates last get operation on queue is complete.
        task_queue.task_done()
        # Simulate slowness.
        sleep(randint(1, 5))

    def get_worker_state(self, job_id) -> WorkerMetadata:
        return workers_state.get((self.id,job_id), None)

    def update_worker_state(self, cur_state) -> bool:
        workers_state[(self.id, cur_state.job_id)] = cur_state
        return True
    
    # This operation access state of all workers responsible for a job.
    # TODO: this can be improved.
    def get_average_of_full_sum(self, job_id) -> pd.DataFrame:
        total_sum = workers_state[(self.id, job_id)].partial_sum_df
        job_worker_ids = jobs_state[job_id].workers
        for worker_id in job_worker_ids:
            worker_state_key = (worker_id, job_id)
            if workers_state.get(worker_state_key) is None:
                raise Exception(f'Worker {worker_id} state not found for job {job_id}')
            state = workers_state[worker_state_key]
            total_sum += state.partial_sum_df
        return total_sum / jobs_state[job_id].total_files

    # This operation access state of all workers responsible for a job.
    # TODO: this can be improved.
    def get_files_processed(self, job_id) -> bool:
        total_files_processed = 0
        job_worker_ids = jobs_state[job_id].workers
        for worker_id in job_worker_ids:
            worker_state_key = (worker_id, job_id)
            if workers_state.get(worker_state_key) is None:
                continue
            state = workers_state[worker_state_key]
            total_files_processed += state.files_processed
        return total_files_processed

def setup_workers() -> None:
    # Create a thread pool with the specified maximum number of workers
    # Start the workers
    workers = [Worker() for _ in range(MAX_WORKERS)]
    for worker in workers:
        global_thread_pool.submit(worker.run)