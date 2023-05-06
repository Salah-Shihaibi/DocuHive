import base64
import cv2
from DocuHive.database.models import DataDB, LabelDB, JobDB
from DocuHive.tasks.task_utils import (
    create_data_info_and_tags,
    base64_string_to_fitz_doc,
    base46_string_to_cv2_image,
)


def tag_extractor_method(tag_extractor, label_name, doc_id, job_id, debug=False, debug_tags=None):
    if job_id is None and not debug:
        raise ValueError("workflow was not passed")
    doc = DataDB.query.get(doc_id)

    if doc.pdf_blob is None:
        base64_data = doc.image_blob.split(",")[1]
        cv2_image = base46_string_to_cv2_image(base64_data)
        results = tag_extractor.run(
            doc=None,
            combined_image=cv2_image,
            file_name=doc.name,
            page_dimensions=doc.page_dimensions,
            debug=debug,
        )
    else:
        cv2_image = base46_string_to_cv2_image(doc.image_blob)
        fitz_doc = base64_string_to_fitz_doc(doc.pdf_blob)
        results = tag_extractor.run(
            doc=fitz_doc,
            combined_image=cv2_image,
            file_name=doc.name,
            page_dimensions=doc.page_dimensions,
            debug=debug,
        )

    if debug:
        debug_img = tag_extractor.debug(debug_tags=debug_tags)
        _, img_png = cv2.imencode(".png", debug_img)
        debug_img_base64 = base64.b64encode(img_png).decode("utf-8")
        return debug_img_base64

    job = JobDB.query.get(job_id)
    label = LabelDB.query.filter(LabelDB.name == label_name).first()
    for tag in results:
        create_data_info_and_tags(
            doc=doc,
            job=job,
            info=tag.data_value,
            polygons_relative_to_the_page=tag.polygons,
            tag_name=tag.tag_name,
            label=label,
        )
    return f"{label_name} {doc.name} done"


def run_extractor(tag_extractor, doc_name, pdf_blob, image_blob, page_dimensions, debug, debug_tags):
    if pdf_blob is None:
        base64_data = image_blob.split(",")[1]
        cv2_image = base46_string_to_cv2_image(base64_data)
        results = tag_extractor.run(
            doc=None,
            combined_image=cv2_image,
            file_name=doc_name,
            page_dimensions=page_dimensions,
            debug=debug,
        )
    else:
        cv2_image = base46_string_to_cv2_image(image_blob)
        fitz_doc = base64_string_to_fitz_doc(pdf_blob)
        results = tag_extractor.run(
            doc=fitz_doc,
            combined_image=cv2_image,
            file_name=doc_name,
            page_dimensions=page_dimensions,
            debug=debug,
        )

    if debug:
        debug_img = tag_extractor.debug(debug_tags=debug_tags)
        _, img_png = cv2.imencode(".png", debug_img)
        debug_img_base64 = base64.b64encode(img_png).decode("utf-8")
        return debug_img_base64

    return results
