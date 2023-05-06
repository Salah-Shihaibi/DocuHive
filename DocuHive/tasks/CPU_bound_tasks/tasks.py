import pickle

from DocuHive.data_science.doc_data_extraction.semantic_search import prep_semantic_search_vector
from DocuHive.tasks.task_setups import celery, CpuTasks
from DocuHive.tasks.workflow_utils import run_extractor
from DocuHive.data_science.scripts.cv_extractor import CvDataExtractor
from DocuHive.data_science.scripts.movie_extractor import MovieDataExtractor
from DocuHive.tasks.IO_bound_tasks.tasks import store_tagged_data, store_one_tagged_data


@celery.task(base=CpuTasks)
def generic_cv_extractor2(doc_id, doc_name, pdf_blob, image_blob, page_dimensions, debug=False, debug_tags=None):
    task_id = generic_cv_extractor2.request.id
    return cpu_bound_extractor(
        CvDataExtractor,
        debug,
        debug_tags,
        doc_id,
        task_id,
        doc_name,
        image_blob,
        page_dimensions,
        pdf_blob,
        "generic_cv_extractor2",
        "cv",
    )


@celery.task(base=CpuTasks)
def movie_extractor2(doc_id, doc_name, pdf_blob, image_blob, page_dimensions, debug=False, debug_tags=None):
    task_id = movie_extractor2.request.id
    return cpu_bound_extractor(
        MovieDataExtractor,
        debug,
        debug_tags,
        doc_id,
        task_id,
        doc_name,
        image_blob,
        page_dimensions,
        pdf_blob,
        "movie_extractor2",
        "movie",
    )


def cpu_bound_extractor(
    tag_extractor,
    debug,
    debug_tags,
    doc_id,
    task_id,
    doc_name,
    image_blob,
    page_dimensions,
    pdf_blob,
    workflow_name,
    label_name,
):
    res = run_extractor(
        tag_extractor=tag_extractor(doc_id=int(doc_id)),
        doc_name=doc_name,
        pdf_blob=pdf_blob,
        image_blob=image_blob,
        page_dimensions=page_dimensions,
        debug=debug,
        debug_tags=debug_tags,
    )
    if debug:
        return res
    store_tagged_data.apply_async(
        args=(res, doc_id, task_id, label_name, f"{workflow_name} result is tagged and stored"), queue="io"
    )
    return f"{doc_name} data is ready for storage"


@celery.task
def prep_semantic_search_vector_task(semantic_search_text, file_name, doc_id, job_id, label_name):
    vec = prep_semantic_search_vector(semantic_search_text)
    store_one_tagged_data.delay(vec, doc_id, job_id, label_name, f"{file_name} serialized_vec is stored and tagged")
    return f"{file_name} serialized_vec is ready"
