import time
from celery import Celery
from DocuHive.database.models import JobDB
from DocuHive.database.setup import db
import DocuHive.main as setup


celery = Celery("celery1", broker="amqp://localhost", backend="rpc://")
celery.conf.update(setup.app.config["CELERY_CONFIG"])
celery.conf.accept_content = ["application/json", "application/x-python-serialize"]
celery.conf.task_routes = ([("CPU_bound_tasks.*", {"queue": "cpu"}), ("IO_bound_tasks.*", {"queue": "io"})],)


class WorkflowTask(celery.Task):
    def __call__(self, *args, **kwargs):
        if kwargs.get("debug", None) is not None:
            with setup.app.app_context():
                return self.run(*args, **kwargs)
        task_id = str(self.request.id)
        with setup.app.app_context():
            time.sleep(0.15)
            job = JobDB.query.filter(JobDB.task_id == task_id).first()
            if job is None:
                raise ValueError(f"job with task id {task_id} does not exist")
            kwargs["job_id"] = job.id
            job.status = "Running"
            db.session.commit()
            return self.run(*args, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        if kwargs.get("debug", None) is not None:
            return super(WorkflowTask, self).on_success(retval, task_id, args, kwargs)
        with setup.app.app_context():
            job = JobDB.query.filter(JobDB.task_id == task_id).first()
            if job is not None:
                job.status = "Done"
                db.session.commit()
                return super(WorkflowTask, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if kwargs.get("debug", None) is not None:
            return super(WorkflowTask, self).on_failure(exc, task_id, args, kwargs, einfo)
        with setup.app.app_context():
            job = JobDB.query.filter(JobDB.task_id == task_id).first()
            if job is not None:
                job.status = "Fail"
                db.session.commit()
                return super(WorkflowTask, self).on_failure(exc, task_id, args, kwargs, einfo)


class TaggedDataStorage(celery.Task):
    def on_success(self, retval, task_id, args, kwargs):
        with setup.app.app_context():
            job = JobDB.query.filter(JobDB.task_id == args[2]).first()
            if job is not None:
                job.status = "Done"
                db.session.commit()
                return super(TaggedDataStorage, self).on_success(retval, task_id, args, kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        with setup.app.app_context():
            job = JobDB.query.filter(JobDB.task_id == args[2]).first()
            if job is not None:
                job.status = "Fail"
                db.session.commit()
                return super(TaggedDataStorage, self).on_failure(exc, task_id, args, kwargs, einfo)


class CpuTasks(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        with setup.app.app_context():
            job = JobDB.query.filter(JobDB.task_id == task_id).first()
            if job is not None:
                job.status = "Fail"
                db.session.commit()
                return super(CpuTasks, self).on_failure(exc, task_id, args, kwargs, einfo)
