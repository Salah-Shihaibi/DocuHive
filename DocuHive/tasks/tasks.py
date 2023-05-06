# import pickle
# import numpy as np
# import DocuHive.main as setup
# from DocuHive.data_science.doc_data_extraction.semantic_search import prep_semantic_search_vector
# from DocuHive.database.models import DataDB, JobDB, LabelDB
# from DocuHive.tasks.task_setups import celery, WorkflowTask
# from DocuHive.tasks.workflow_utils import tag_extractor_method, run_extractor
# from DocuHive.data_science.scripts.cv_extractor import CvDataExtractor
# from DocuHive.data_science.scripts.movie_extractor import MovieDataExtractor
# from DocuHive.tasks.task_utils import create_data_info_and_tags
#
#
# @celery.task(base=WorkflowTask)
# def generic_cv_extractor(doc_id, job_id=None, debug=False, debug_tags=None):
#     return tag_extractor_method(
#         tag_extractor=CvDataExtractor(doc_id=int(doc_id), job_id=int(job_id)),
#         label_name="cv",
#         doc_id=doc_id,
#         job_id=job_id,
#         debug=debug,
#         debug_tags=debug_tags,
#     )
#
#
# @celery.task(base=WorkflowTask)
# def movie_extractor(doc_id, job_id=None, debug=False, debug_tags=None):
#     return tag_extractor_method(
#         tag_extractor=MovieDataExtractor(doc_id=int(doc_id), job_id=int(job_id)),
#         label_name="movie",
#         doc_id=doc_id,
#         job_id=job_id,
#         debug=debug,
#         debug_tags=debug_tags,
#     )
#
#
# @celery.task
# def generic_cv_extractor2(doc_id, doc_name, pdf_blob, image_blob, page_dimensions, debug=False, debug_tags=None):
#     task_id = generic_cv_extractor2.request.id
#     run_extractor(
#         tag_extractor=CvDataExtractor(doc_id=int(doc_id)),
#         doc_name=doc_name,
#         pdf_blob=pdf_blob,
#         image_blob=image_blob,
#         page_dimensions=page_dimensions,
#         debug=debug,
#         debug_tags=debug_tags,
#     )
#
#
# @celery.task
# def store_tagged_data(info, doc_id, job_id, label_name, return_msg):
#     with setup.app.app_context():
#         doc = DataDB.query.get(doc_id)
#         job = JobDB.query.get(job_id)
#         label = LabelDB.query.filter(LabelDB.name == label_name).first()
#         create_data_info_and_tags(
#             doc=doc,
#             job=job,
#             info=pickle.dumps(np.array(info)),
#             polygons_relative_to_the_page=None,
#             tag_name="Semantic Search Blob",
#             label=label,
#         )
#     return return_msg
#
#
# @celery.task
# def prep_semantic_search_vector_task(semantic_search_text, file_name, doc_id, job_id, label_name):
#     vec = prep_semantic_search_vector(semantic_search_text)
#     store_tagged_data.delay(vec, doc_id, job_id, label_name, f"{file_name} serialized_vec is stored and tagged")
#     return f"{file_name} serialized_vec is ready"
