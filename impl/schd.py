import threading
from time import sleep
from impl.consts import Status


class Scheduler:
    def __init__(self):
        # In memory state. Could be maintained in a database.
        self.jobs = {}
        self.job_threds = []
    
    def get_job(self, job_id):
        return self.jobs.get(job_id, None)

    # Submitted by an external user.
    def add_job(self, job):
        self.jobs[job.id] = job
    
    def run(self):
        while 1:
            for job_id in self.jobs:
                job = self.jobs[job_id]
                if job.status == Status.PENDING:
                    th = threading.Thread(target=job.run, args=())
                    th.start()
                    self.job_threds.append(th)
            sleep(1)

# Singleton instance of the scheduler.
scheduler = Scheduler()