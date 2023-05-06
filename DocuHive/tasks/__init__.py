from DocuHive.tasks.IO_bound_tasks.tasks import (
    generic_cv_extractor,
    movie_extractor,
    store_tagged_data,
    store_one_tagged_data,
)

from DocuHive.tasks.CPU_bound_tasks.tasks import (
    generic_cv_extractor2,
    movie_extractor2,
    prep_semantic_search_vector_task,
)

__all__ = [
    "generic_cv_extractor2",
    "movie_extractor2",
    "store_one_tagged_data",
    "generic_cv_extractor",
    "movie_extractor",
    "store_tagged_data",
    "prep_semantic_search_vector_task",
]


# from sqlalchemy.exc import PendingRollbackError, InvalidRequestError, OperationalError, DatabaseError, ProgrammingError
# from psycopg2 import Error
# from sqlalchemy.orm.exc import StaleDataError

# database_errors = (Error, OperationalError, DatabaseError, ProgrammingError, StaleDataError, NotImplementedError)

# class WorkflowTask(celery.Task):
#     def __call__(self, *args, **kwargs):
#         if kwargs.get("debug", None) is not None:
#             return self.run(*args, **kwargs)
#         task_id = str(self.request.id)
#         with a.app.app_context():
#             time.sleep(0.1)
#             while True:
#                 time.sleep(0.05)
#                 try:
#                     job = JobDB.query.filter(JobDB.task_id == task_id).first()
#                     if job is None:
#                         raise ValueError(f"job with task id {task_id} does not exist")
#                     kwargs["job_id"] = job.id
#                     job.status = "Running"
#                     db.session.commit()
#                     return self.run(*args, **kwargs)
#                 except PendingRollbackError:
#                     try:
#                         db.session.rollback()
#                     except (InvalidRequestError,) + database_errors:
#                         pass
#                 except InvalidRequestError:
#                     try:
#                         db.session.commit()
#                         return self.run(*args, **kwargs)
#                     except (PendingRollbackError,) + database_errors:
#                         pass
#                 except database_errors:
#                     pass
#
#     def on_success(self, retval, task_id, args, kwargs):
#         """
#         retval – The return value of the task.
#         task_id – Unique id of the executed task.
#         args – Original arguments for the executed task.
#         kwargs – Original keyword arguments for the executed task.
#         """
#         if kwargs.get("debug", None) is not None:
#             return super(WorkflowTask, self).on_success(retval, task_id, args, kwargs)
#         with a.app.app_context():
#             while True:
#                 time.sleep(0.05)
#                 try:
#                     job = JobDB.query.filter(JobDB.task_id == task_id).first()
#                     if job is not None:
#                         job.status = "Done"
#                         db.session.commit()
#                         return super(WorkflowTask, self).on_success(retval, task_id, args, kwargs)
#                 except PendingRollbackError:
#                     try:
#                         db.session.rollback()
#                     except (InvalidRequestError,) + database_errors:
#                         pass
#                 except InvalidRequestError:
#                     try:
#                         db.session.commit()
#                         return super(WorkflowTask, self).on_success(retval, task_id, args, kwargs)
#                     except (PendingRollbackError,) + database_errors:
#                         pass
#                 except database_errors:
#                     pass
#
#     def on_failure(self, exc, task_id, args, kwargs, einfo):
#         """
#         exc – The exception raised by the task.
#         task_id – Unique id of the failed task.
#         args – Original arguments for the task that failed.
#         kwargs – Original keyword arguments for the task that failed.
#         """
#         if kwargs.get("debug", None) is not None:
#             return super(WorkflowTask, self).on_failure(exc, task_id, args, kwargs, einfo)
#         with a.app.app_context():
#             while True:
#                 time.sleep(0.05)
#                 try:
#                     job = JobDB.query.filter(JobDB.task_id == task_id).first()
#                     if job is not None:
#                         job.status = "Fail"
#                         db.session.commit()
#                         if hasattr(einfo.exception, "args") and len(einfo.exception.args) == 1:
#                             job.status_message = einfo.exception.args[0]
#                         return super(WorkflowTask, self).on_failure(exc, task_id, args, kwargs, einfo)
#                 except PendingRollbackError:
#                     try:
#                         db.session.rollback()
#                     except (InvalidRequestError,) + database_errors:
#                         pass
#                 except InvalidRequestError:
#                     try:
#                         db.session.commit()
#                         if hasattr(einfo.exception, "args") and len(einfo.exception.args) == 1:
#                             job.status_message = einfo.exception.args[0]
#                         return super(WorkflowTask, self).on_failure(exc, task_id, args, kwargs, einfo)
#                     except (PendingRollbackError,) + database_errors:
#                         pass
#                 except database_errors:
#                     pass
