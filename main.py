import threading
import os
from impl.utils import get_csv_file_paths
from impl.schd import scheduler
from impl.processor.processor import get_job
from impl.consts import JOB_DISPATCH_ALGO, JOB_ALGO, BATCH_SIZE

def main():
    print("Starting scheduler service")
    scheduler_thread = threading.Thread(target=scheduler.run)
    # Scheduler runs in a separate thread as a blocking operation.
    scheduler_thread.start()

    if JOB_DISPATCH_ALGO == JOB_ALGO.SCATTER_GATHER and BATCH_SIZE < 2:
        raise ValueError('Batch size must be >= 2 for scatter-gather job dispatch algorithm')

    # Add work to the scheduler.
    
    relative_input_path = './data/input_1/'
    relative_output_path = './data/input_1/output'
    input_dir =  os.path.abspath(relative_input_path)
    output_dir = os.path.abspath(relative_output_path)

    num_files = len(get_csv_file_paths(input_dir))
    cardinality = 3
    num_workers = 2
    job = get_job("test_job_1", num_workers, num_files, cardinality, input_dir, output_dir)
    
    print(f'Adding job {job}')
    scheduler.add_job(job)

    scheduler_thread.join()

if __name__ == "__main__":
    main()