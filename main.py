import threading
from impl.schd import scheduler
from impl.consts import JOB_DISPATCH_ALGO, JOB_ALGO, BATCH_SIZE

from app.router import server


def start_scheduler():
    print("Starting scheduler service")
    # Scheduler runs in a separate thread as a blocking operation.
    threading.Thread(target=scheduler.run).start()
    print("Scheduler listening for jobs")

def main():
    if JOB_DISPATCH_ALGO == JOB_ALGO.SCATTER_GATHER and BATCH_SIZE < 2:
        raise ValueError('Batch size must be >= 2 for scatter-gather job dispatch algorithm')

    start_scheduler()
    server.run(debug=True)
    
if __name__ == "__main__":
    main()
