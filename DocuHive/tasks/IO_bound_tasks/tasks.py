import pickle
import numpy as np
import DocuHive.main as setup
from DocuHive.database.models import DataDB, JobDB, LabelDB
from DocuHive.tasks.task_setups import celery, WorkflowTask, TaggedDataStorage
from DocuHive.tasks.workflow_utils import tag_extractor_method
from DocuHive.data_science.scripts.cv_extractor import CvDataExtractor
from DocuHive.data_science.scripts.movie_extractor import MovieDataExtractor
from DocuHive.tasks.task_utils import create_data_info_and_tags
from DocuHive.data_science.scripts.doc_extractor import TagData


@celery.task(base=WorkflowTask)
def generic_cv_extractor(doc_id, job_id=None, debug=False, debug_tags=None):
    return tag_extractor_method(
        tag_extractor=CvDataExtractor(doc_id=int(doc_id), job_id=int(job_id)),
        label_name="cv",
        doc_id=doc_id,
        job_id=job_id,
        debug=debug,
        debug_tags=debug_tags,
    )


@celery.task(base=WorkflowTask)
def movie_extractor(doc_id, job_id=None, debug=False, debug_tags=None):
    return tag_extractor_method(
        tag_extractor=MovieDataExtractor(doc_id=int(doc_id), job_id=int(job_id)),
        label_name="movie",
        doc_id=doc_id,
        job_id=job_id,
        debug=debug,
        debug_tags=debug_tags,
    )


@celery.task(base=TaggedDataStorage, serializer="pickle")
def store_tagged_data(tagged_data, doc_id, task_id, label_name, return_msg):
    with setup.app.app_context():
        doc = DataDB.query.get(doc_id)
        job = JobDB.query.filter(JobDB.task_id == task_id).first()
        label = LabelDB.query.filter(LabelDB.name == label_name).first()
        for tag in tagged_data:
            create_data_info_and_tags(
                doc=doc,
                job=job,
                info=tag.data_value,
                polygons_relative_to_the_page=tag.polygons,
                tag_name=tag.tag_name,
                label=label,
            )
        return return_msg


@celery.task
def store_one_tagged_data(info, doc_id, job_id, label_name, return_msg):
    with setup.app.app_context():
        doc = DataDB.query.get(doc_id)
        job = JobDB.query.get(job_id)
        label = LabelDB.query.filter(LabelDB.name == label_name).first()
        create_data_info_and_tags(
            doc=doc,
            job=job,
            info=pickle.dumps(np.array(info)),
            polygons_relative_to_the_page=None,
            tag_name="Semantic Search Blob",
            label=label,
        )
    return return_msg
