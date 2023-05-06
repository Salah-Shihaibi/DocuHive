import pdf2image
import base64
import io

from PIL import Image
from flask import (
    request,
    abort,
    Blueprint,
    make_response,
    jsonify,
)
from sqlalchemy.orm import load_only

from DocuHive.backend_utils.seed import seed_tags_labels_and_workflows
from DocuHive.backend_utils.utils import (
    queue_job,
    check_if_data_exists,
    delete_job,
    get_concat_v,
    get_file_type,
    get_debug_data,
)
from DocuHive.backend_utils.view_workflow import TreeGraph
from DocuHive.database.models import DataDB, JobDB, DataType
from DocuHive.database.setup import db

bp = Blueprint("root", __name__)


def message_abort(code, message):
    abort(make_response(jsonify(message=message), code))


@bp.route("/")
def running():
    return "DocuHive backend running..."


@bp.route("/upload_file", methods=["POST"])
def upload():
    content = request.get_json(silent=True)
    doc_name = content["fileName"]
    blob = content["fileBase64String"]
    file_type = content["fileType"]
    res = DataDB.query.filter(DataDB.name == doc_name).options(load_only(DataDB.id)).first()
    if res is not None:
        message_abort(400, f"doc with name: {doc_name} exists, upload cancelled")

    if file_type.endswith("pdf"):
        base64_data = blob.split(",")[1]
        base64_bytes = base64.b64decode(base64_data)
        pil_images = pdf2image.convert_from_bytes(base64_bytes)
        concatenated_image = pil_images[0]
        width, height = pil_images[0].size
        page_dimensions = f"{width}x{height}"
        for i in range(1, len(pil_images)):
            width, height = pil_images[i].size
            page_dimensions += f":{width}x{height}"
            concatenated_image = get_concat_v(concatenated_image, pil_images[i])
        jpeg_bytes = io.BytesIO()
        concatenated_image.save(jpeg_bytes, format="PNG")
        jpeg_bytes = jpeg_bytes.getvalue()
        base64_jpeg = base64.b64encode(jpeg_bytes).decode("utf-8")
        image_blob = base64_jpeg
        pdf_blob = blob
    else:
        image_blob = blob
        image_data = base64.b64decode(image_blob.split(",")[1])
        img = Image.open(io.BytesIO(image_data))
        page_dimensions = f"{img.width}x{img.height}"
        pdf_blob = None

    data = DataDB(
        name=doc_name,
        image_blob=image_blob,
        pdf_blob=pdf_blob,
        page_dimensions=page_dimensions,
        data_type=DataType.file,
    )
    db.session.add(data)
    db.session.commit()
    return {"data_id": str(data.id)}


@bp.route("/queue_job", methods=["POST"])
def queue_workflow_endpoint():
    content = request.get_json(silent=True)
    job_name = content["jobName"]
    doc_id = content["dataId"]
    if not check_if_data_exists([doc_id]):
        abort(400, f"doc {doc_id} does not exist.")
    extra_arguments = content.get("extraArguments", {})
    if job_name is not None:
        job = queue_job(doc_id, extra_arguments, job_name, abort)
        return_val = {"job_identifier": job.identifier, "job_id": str(job.id)}
        return return_val
    else:
        abort(400, "job_name not passed")


@bp.route("/delete_doc", methods=["DELETE"])
def delete_doc():
    content = request.get_json(silent=True)
    doc_id = content["doc_id"]
    doc_data = DataDB.query.get(doc_id)

    if doc_data is None:
        abort(404, f"Document with id {doc_id} not found")

    db.session.delete(doc_data)
    db.session.commit()
    return f"Document with id {doc_id} has been successfully deleted along with all associated data."


@bp.route("/delete_job", methods=["DELETE"])
def delete_job_endpoint():
    content = request.get_json(silent=True)
    job_id = content["job_id"]
    job_id_int = int(job_id) if job_id.isdigit() else -1
    job = JobDB.query.filter((JobDB.id == job_id_int) | (JobDB.identifier == job_id)).first()
    if job is None:
        abort(404, f"Job with id {job} not found")

    delete_job(job)
    return f"Job {job.identifier} has been successfully deleted along with all associated data."


@bp.route("/delete_all_data", methods=["DELETE"])
def delete_all_data():
    for table_name in [
        "data_tag_data_db",
        "data_job_table",
        "job_data_table",
        "data_db",
        "job_db",
        "workflow_db",
    ]:
        model = db.Model.metadata.tables[table_name]
        db.session.query(model).delete()
    db.session.commit()
    seed_tags_labels_and_workflows()
    return "Done"


@bp.route("/get_debug_data", methods=["POST"])
def debug_data():
    content = request.get_json(silent=True)
    job_name = content["jobName"]
    doc_id = content["dataId"]
    debug_tags = content["debugTags"]
    if not check_if_data_exists([doc_id]):
        abort(400, f"doc {doc_id} does not exist.")
    extra_arguments = content.get("extraArguments", {})
    if job_name is not None:
        task_id = get_debug_data(doc_id, debug_tags, extra_arguments, job_name, abort)
        return {"task_id": str(task_id)}
    else:
        abort(400, "job_name not passed")


@bp.route("/get_task_output", methods=["POST"])
def get_task_output():
    from DocuHive.tasks.task_setups import celery

    content = request.get_json(silent=True)
    task_id = content["taskId"]
    result = celery.AsyncResult(task_id)
    if result.successful():
        return {"res": result.get()}
    elif result.failed():
        return {"res": "failed"}
    else:
        return {"res": "loading"}


@bp.route("/print_graph", methods=["POST"])
def print_graph():
    content = request.get_json(silent=True)
    graph_name = content["file_name"]
    TG = TreeGraph()
    TG.seed_graph("trees/" + graph_name)
    return "Done"
