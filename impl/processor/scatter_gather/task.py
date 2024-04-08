import os
import uuid
import pandas as pd

from impl.consts import Status, Operation

class Task:
    def __init__(self, job_id, level, input_files, output_directory, total_files, operation=None):
        self.id = uuid.uuid4()  # Should be generated in a DB.
        self.job_id = job_id
        self.level = level
        
        self.input_files = input_files
        self.output_directory = output_directory
        self.total_files = total_files
        
        self.operation = operation or Operation.SUM
        self.status = Status.PENDING
    
    def __str__(self):
        return f'Task: {self.job_id}, {self.level}'
    

    def run(self):
        print(f'Worker {self.id} is running task {self.id} with input files {self.input_files} and total files {self.total_files}')
        # Each task has a set of csv files that store a vector
        # read these files and perform sum operation on the vectors
        
        result = pd.read_csv(self.input_files[0], header=None) 
        for file in self.input_files[1:]:
            df = pd.read_csv(file, header=None)
            print(f'Processing file {file} with df {df}')
            result += df
        
        if self.operation == Operation.AVG:
            result = result / self.total_files

        print(f'Task {self.id} completed. Result: {result}')
        
        # Write results in a csv file at task output location
        output_file = f'{self.output_directory}/{self.id}.csv'
        result.to_csv(output_file, index=False, header=None)
        print(f'Output written to {output_file}')

        # Update the input files with a marker to indicate completion.
        # Needed for idempotancy.
        for file in self.input_files:
            os.rename(file, f'{file}.done')
        
        self.status = Status.COMPLETE
