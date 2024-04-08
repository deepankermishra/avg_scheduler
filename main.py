import threading
import os
from impl.utils import get_csv_files
from impl.schd import scheduler
from impl.job.job import Job

def main():
    print("Starting scheduler service")
    scheduler_thread = threading.Thread(target=scheduler.run)
    # Scheduler runs in a separate thread as a blocking operation.
    scheduler_thread.start()

    # Add work to the scheduler.
    
    relative_path = './data/input_1'
    input_dir =  os.path.abspath(relative_path)
    num_files = len(get_csv_files(input_dir))
    cardinality = 3
    num_workers = 2
    job = Job("test_job_1", num_workers, num_files, cardinality, input_dir)
    print(f'Adding job {job}')
    scheduler.add_job(job)

    scheduler_thread.join()

if __name__ == "__main__":
    main()