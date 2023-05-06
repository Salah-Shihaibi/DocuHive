import base64
import pickle

import cv2
import fitz
import numpy as np

from DocuHive.database.models import JobDB, TagDB, DataDB, DataTagDataDB
from DocuHive.database.setup import db


def append_data_to_job(job_id, datas):
    job = JobDB.query.get(job_id)
    job.datas.append(**datas)
    db.session.add(job)
    return job


def boolean(value):
    if isinstance(value, bool):
        return value

    valid = {
        "true": True,
        "t": True,
        "1": True,
        "yes": True,
        "false": False,
        "f": False,
        "0": False,
        "no": False,
    }

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    raise ValueError('invalid literal for boolean: "%s"' % value)


def ml_data_type_validation(input_data, expected_data_type):
    if input_data is None:
        return None
    try:
        if expected_data_type == "float":
            return float(input_data)
        if expected_data_type == "integer":
            return int(input_data)
        if expected_data_type == "boolean":
            return boolean(input_data)
        if expected_data_type == "text":
            return str(input_data)
        if expected_data_type == "blob":
            return pickle.dumps(input_data)
    except ValueError:
        print(f"Error: '{input_data}' must be a '{expected_data_type}'.")
        return None


def create_data_info_and_tags(doc, label, job, info, polygons_relative_to_the_page, tag_name, collection=None):
    job_name = job.workflow.name
    tag = TagDB.query.filter(TagDB.name == tag_name).first()
    if tag is None:
        raise ValueError(f"{tag_name} not found in TagDB")
    if label is None:
        raise ValueError(f"{label} not found in LabelDB")
    polygons_string = None
    if polygons_relative_to_the_page:
        concatenated_polygons = [",".join(list(map(str, inner_list))) for inner_list in polygons_relative_to_the_page]
        polygons_string = "x".join(concatenated_polygons)
    info = ml_data_type_validation(info, tag.data_type.name)
    data_type = {tag.data_type.name: info, "data_type": tag.data_type}
    data = DataDB(**data_type, polygons=polygons_string)
    job.datas.append(data)
    data_tag_data = DataTagDataDB(
        parent_file_data=doc,
        parent_collection_data=collection,
        data=data,
        tag=tag,
        label=label,
        job_name=job_name,
    )
    db.session.add(data_tag_data)
    db.session.commit()


def base64_string_to_fitz_doc(pdf_base64):
    base64_data = pdf_base64.split(",")[1]
    pdf_bytes = base64.b64decode(base64_data)
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    return doc


def base46_string_to_cv2_image(base64_string):
    img_bytes = base64.b64decode(base64_string)
    img_array = np.frombuffer(img_bytes, dtype=np.uint8)
    return cv2.imdecode(img_array, flags=cv2.IMREAD_COLOR)


def get_doc_page_dimensions(fitz_doc):
    page_dimensions = []
    for page in fitz_doc:
        width, height = page.rect.width, page.rect.height
        page_dimensions.append(f"{width}x{height}")
    page_dimensions_str = ",".join(page_dimensions)
    return page_dimensions_str
