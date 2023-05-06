import os
import io
import pandas as pd
import base64


def upload_and_process_docs(client, workflow_name, data_file):
    job_ids = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    image_dir = f"{script_dir}/data/{data_file}"
    image_files = [os.path.join(image_dir, f) for f in os.listdir(image_dir)]
    for file_path in image_files:
        b64_str = file_to_base64(file_path)
        doc_id = client.upload_file(file_path.split("/")[-1], b64_str, file_path.split(".")[-1])
        job_id = client.queue_job(workflow_name, doc_id)
        job_ids.append(int(job_id))
    return job_ids


def file_to_base64(file_path):
    with open(file_path, "rb") as file:
        file_content = file.read()
        return base64.b64encode(file_content).decode("utf-8")


def save_csv(csv, filename="output.csv"):
    df = pd.read_csv(io.StringIO(csv))
    df.to_csv(filename, index=False)


def semantic_search_results(client, expected_order, search_query, num_results=None, label_name="cv"):
    semantic_search_res = client.semantic_search(label_name=label_name, search=search_query, num_results=num_results)
    result_order = [semantic_data["data"]["name"] for semantic_data in semantic_search_res]
    assert set(result_order) == set(expected_order), f"Expected order {expected_order}, but got {result_order}"
