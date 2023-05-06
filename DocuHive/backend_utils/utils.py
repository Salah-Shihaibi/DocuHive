import base64
import re

from PIL import Image

from DocuHive.database.models import DataDB, JobDB, DataType, WorkflowDB
from DocuHive.database.setup import db


def is_list_of_ints(arg):
    if not isinstance(arg, list):
        return False
    return all(isinstance(elem, int) for elem in arg)


def check_if_data_exists(data_ids):
    for data_id in data_ids:
        if DataDB.query.get(data_id) is None:
            return False
    return True


def get_identifier(job_name, data_inputs_ids, extra_arguments):
    if not isinstance(data_inputs_ids, list):
        data_inputs_ids = [data_inputs_ids]
    identifier_list = [job_name] + data_inputs_ids
    for k, v in extra_arguments.items():
        identifier_list.extend([k, v])
    identifier_list = list(map(str, identifier_list))
    return "--".join(identifier_list)


def check_if_job_exists(job_name, data_inputs_ids, extra_arguments, abort):
    identifier = get_identifier(job_name, data_inputs_ids, extra_arguments)
    job = JobDB.query.filter(JobDB.identifier == identifier).order_by(JobDB.time_created.desc()).first()
    loading_states = ["Queued", "Running"]
    if job is not None:
        if job.status == "Fail":
            delete_job(job)
        elif job.status in loading_states:
            return abort(400, "workflow already running")
        else:
            return abort(
                400,
                f"job with identifier {identifier} already exists. workflow completed successfully",
            )
    return identifier


def register_job(identifier, task_id, job_name, data_inputs_ids, extra_arguments=""):
    workflow = WorkflowDB.query.filter(WorkflowDB.name == job_name).first()
    job = JobDB(
        identifier=identifier,
        task_id=str(task_id),
        status="Queued",
        workflow=workflow,
        arguments=str(extra_arguments),
    )
    for data_id in data_inputs_ids:
        data = DataDB.query.get(data_id)
        data.jobs.append(job)
    db.session.add(job)
    db.session.commit()
    return job


def get_doc_data(doc_id):
    doc = DataDB.query.get(doc_id)
    return doc.id, doc.name, doc.pdf_blob, doc.image_blob, doc.page_dimensions


def queue_job(doc_id, extra_arguments, job_name, abort):
    from DocuHive.tasks.task_setups import celery

    tasks = celery.tasks
    task = None
    for key in tasks:
        if key.endswith(f".{job_name}"):
            task = tasks[key]
            task_location = key
            break

    if task is None:
        return abort(400, f"Job {job_name} not found in tasks")
    identifier = check_if_job_exists(job_name, doc_id, extra_arguments, abort)
    if task_location.split(".")[-3] == "CPU_bound_tasks":
        doc_data = get_doc_data(doc_id)
        task_id = task.apply_async(args=doc_data, kwargs=extra_arguments, queue="cpu")
    else:
        task_id = task.apply_async(args=(doc_id,), kwargs=extra_arguments, queue="io")
    return register_job(identifier, task_id, job_name, data_inputs_ids=[doc_id], extra_arguments="")


def get_debug_data(doc_id, debug_tags, extra_arguments, job_name, abort):
    from DocuHive.tasks.task_setups import celery

    tasks = celery.tasks
    task = None
    for key in tasks:
        if key.endswith(f".{job_name}"):
            task = tasks[key]
            task_location = key
            break

    if task is None:
        return abort(400, f"Job {job_name} not found in tasks")
    extra_arguments["debug"] = True
    extra_arguments["debug_tags"] = debug_tags
    if task_location.split(".")[-3] == "CPU_bound_tasks":
        doc_data = get_doc_data(doc_id)
        task_id = task.apply_async(args=doc_data, kwargs=extra_arguments, queue="cpu")
    else:
        task_id = task.apply_async(args=(doc_id,), kwargs=extra_arguments, queue="io")
    return task_id


def delete_job(job):
    datas = job.datas_reversed
    for data in datas:
        data.jobs.remove(job)
    db.session.delete(job)
    db.session.commit()


def get_data_id_of_document(data_id=None, job_id=None):
    if data_id:
        data = DataDB.query.get(data_id)
    elif job_id:
        job = JobDB.query.get(job_id)
        data = job.datas_reversed[0]
    else:
        raise TypeError("get_data_id_of_document() is missing both arguments data_id and job_id")

    while True:
        if data.data_type == DataType.document:
            break
        jobs = data.jobs_reverse
        assert len(jobs) != 0, "If data_type is not document then it should have a job"
        job = jobs[0]
        data = job.datas_reversed[0]
    return data.id


def delete_datas_jobs(job_data_list, data_job_toggle, db):
    if job_data_list is None or len(job_data_list) == 0:
        return
    for job_data in job_data_list:
        if data_job_toggle == "data":
            if job_data.file:
                db.session.delete(job_data.file)
            jobs = job_data.jobs_reversed
            for job in jobs:
                job.datas.remove(job_data)
            delete_datas_jobs(job_data.jobs, "job", db)
        elif data_job_toggle == "job":
            datas = job_data.datas_reversed
            for data in datas:
                data.jobs.remove(job_data)
            delete_datas_jobs(job_data.datas, "data", db)
        else:
            raise Exception("Provide data_job_toggle")
    db.session.delete(job_data)


def get_workflow_jobs_status(job_data_list, data_job_toggle):
    jobs = {}
    switch_data_job_toggle = {"data": "job", "job": "data"}
    if isinstance(job_data_list, str):
        job_data_list = [job_data_list]

    while job_data_list is not None and len(job_data_list) != 0:
        new_job_data_list = []
        for job_data in job_data_list:
            if data_job_toggle == "data":
                new_job_data_list.extend(job_data.jobs)
            elif data_job_toggle == "job":
                jobs[job_data.job_name] = job_data.status
                new_job_data_list.extend(job_data.datas)
            else:
                raise Exception("Provide data_job_toggle")
        data_job_toggle = switch_data_job_toggle[data_job_toggle]
        job_data_list = new_job_data_list
    return jobs


def replace_special_chars(tag_name):
    repl_dict = {
        "!": "exclamation_mark",
        "@": "at_sign",
        "#": "hash_symbol",
        "$": "dollar_sign",
        "%": "percent_sign",
        "^": "caret",
        "&": "ampersand",
        "*": "asterisk",
        "(": "left_parenthesis",
        ")": "right_parenthesis",
        "-": "hyphen",
        "+": "plus_sign",
        "=": "equal_sign",
        "[": "left_bracket",
        "]": "right_bracket",
        "{": "left_curly_brace",
        "}": "right_curly_brace",
        "|": "pipe",
        "\\": "backslash",
        ";": "semicolon",
        ":": "colon",
        "'": "apostrophe",
        '"': "quotation_mark",
        ",": "comma",
        ".": "period",
        "<": "less_than_sign",
        ">": "greater_than_sign",
        "?": "question_mark",
        "/": "slash",
        " ": "space",
    }
    pattern = r"[^a-zA-Z0-9_]"
    for char in re.findall(pattern, tag_name):
        if char in repl_dict:
            replacement = f"_0{repl_dict[char]}0_"
        else:
            raise ValueError(f"{char} is not added to the special_chars")
        tag_name = tag_name.replace(char, replacement)
    return "start0_" + tag_name


def get_special_chars(column_name):
    column_name = column_name.replace("start0_", "") if column_name.startswith("start0_") else column_name
    pattern = r"_0\w+?0_"
    repl_dict = {
        "space": " ",
        "exclamation_mark": "!",
        "at_sign": "@",
        "hash_symbol": "#",
        "dollar_sign": "$",
        "percent_sign": "%",
        "caret": "^",
        "ampersand": "&",
        "asterisk": "*",
        "left_parenthesis": "(",
        "right_parenthesis": ")",
        "hyphen": "-",
        "plus_sign": "+",
        "equal_sign": "=",
        "left_bracket": "[",
        "right_bracket": "]",
        "left_curly_brace": "{",
        "right_curly_brace": "}",
        "pipe": "|",
        "backslash": "\\",
        "semicolon": ";",
        "colon": ":",
        "apostrophe": "'",
        "quotation_mark": '"',
        "comma": ",",
        "period": ".",
        "less_than_sign": "<",
        "greater_than_sign": ">",
        "question_mark": "?",
        "slash": "/",
    }
    for char_string in re.findall(pattern, column_name):
        char = repl_dict.get(char_string[2:-2], char_string)
        column_name = column_name.replace(char_string, char)
    return column_name


def get_concat_v(im1, im2):
    gap_size = 10
    dst = Image.new("RGBA", (im1.width, im1.height + im2.height + gap_size), (0, 0, 0, 0))
    dst.paste(im1, (0, 0))
    dst.paste(im2, (0, im1.height + gap_size))
    return dst


def get_file_type(base64_string):
    decoded_data = base64.b64decode(base64_string)
    header = decoded_data[:8]

    if header.startswith(b"\x89PNG"):
        return "PNG"
    elif header.startswith(b"\xFF\xD8"):
        return "JPG"
    elif header.startswith(b"%PDF"):
        return "PDF"
    else:
        return "Unknown"
