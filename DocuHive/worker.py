import random
import string
from DocuHive.tasks.task_setups import celery


def run_worker():
    hostname = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    celery.conf.update(task_track_started=True, broker_heartbeat=300, broker_heartbeat_checkrate=1)
    worker = celery.Worker(loglevel="info", concurrency=1, hostname=hostname)
    worker.start()


if __name__ == "__main__":
    run_worker()
