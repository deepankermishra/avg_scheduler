import os
import queue
import threading
import time
import uuid

import pandas as pd
from impl.consts import Status, BATCH_SIZE, MAX_WORKERS, WAIT_TIME_SEC, MAX_WAIT_TIME_SEC
from impl.processor.job import Job
from impl.utils import get_csv_file_paths
from concurrent.futures import ThreadPoolExecutor



class AccumulatorJob(Job):

    def __init__(self, name, num_workers, num_files, cardinality, input_dir, output_dir=None):
        super().__init__(name, num_workers, num_files, cardinality, input_dir, output_dir)
        self.accumulator = queue.Queue()
        self.num_workers = min(num_workers, MAX_WORKERS)
        self.worker_pool = ThreadPoolExecutor(max_workers=num_workers)
        self.lock = threading.Lock()
        self.num_op = 0

    def run(self):
        print(f'Job {self.id} started')
        self.status = Status.RUNNING

        # Create an output directory for the job.
        output_dir = f'{self.output_dir}/{self.id}'
        os.makedirs(output_dir, exist_ok=True)
        
        # Put work for each file on the accumulator queue.
        file_paths = get_csv_file_paths(self.input_dir)
        if len(file_paths) == 0:
            print(f'No files found to be processed {self.input_dir}')
            return

        for file_path in file_paths:
            self.accumulator.put({
                "file_path": file_path
            })

        self.work_distributer(output_dir)

    def work_distributer(self, output_dir):
        print('Distributing work')
        while not self.is_summation_done():
            # Distribute summing work to the worker pool.
            batch = []
            start_time = time.time()
            while not self.ready_to_submit_batch(len(batch), start_time):
                try:
                    elem = self.accumulator.get_nowait()
                    batch.append(elem["file_path"])
                except queue.Empty:
                    pass
            
            print(f'Sumit batch: {batch}')
            self.worker_pool.submit(self.batch_sum, 
                                    batch,
                                    output_dir)
        
        # Compute average.
        print('Computing average on final file')
        batch = []
        batch.append(self.accumulator.get(block=True)["file_path"])
        self.avg(batch, output_dir)
        
        self.job_state = Status.COMPLETE
        self.worker_pool.shutdown()
        print(f'Job {self.id} completed')

    def is_summation_done(self):
        with self.lock:
            return self.num_op == self.num_files - 1
    
    def ready_to_submit_batch(self, batch_size, start_time):
        time_since = (time.time() - start_time)
        if time_since > MAX_WAIT_TIME_SEC and not self.is_summation_done():
            raise Exception('Wait time exceeded')
        return batch_size == BATCH_SIZE or (1 < batch_size < BATCH_SIZE and time_since > WAIT_TIME_SEC) or self.is_summation_done()

    def batch_sum(self, input_file_paths, output_dir):
        id = uuid.uuid4()

        result = pd.read_csv(input_file_paths[0], header=None)
        for file in input_file_paths[1:]:
            df = pd.read_csv(file, header=None)
            result += df
    
        # Write results in a csv file at output path.
        output_file = f'{output_dir}/_{id}.csv'
        result.to_csv(output_file, index=False, header=None)
        print(f'Output written to {output_file}, result: {result}')
        
        # Mark the queue elem as done.
        for _ in input_file_paths:
            self.accumulator.task_done()
        
        # Put the output file in the queue for further processing.
        self.accumulator.put({
            "file_path": output_file
        })

        with self.lock:
            self.num_op += len(input_file_paths) - 1
    
    def avg(self, input_file_paths, output_dir):
        result = pd.read_csv(input_file_paths[0], header=None)

        for file in input_file_paths[1:]:
            df = pd.read_csv(file, header=None)
            result += df
        
        result /= self.num_files
        
        output_file = f'{output_dir}/result.csv'
        result.to_csv(output_file, index=False, header=None)

        # Mark the queue elem as done.
        for _ in input_file_paths:
            self.accumulator.task_done()
        print(f'Output written to {output_file}, result: {result}')
