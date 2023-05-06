import json
import os
import time

import requests


class DocuHiveClient:
    def __init__(self, host=None, request_session=None):
        if host is None:
            host = os.environ.get("DOCUHIVE_HOST", "http://localhost:8080")
        self.host = host
        self.request_session = request_session
        if request_session is None:
            self.request_session = requests.Session()
        self.headers = {
            "Content-type": "application/json",
            "Accept": "application/json",
        }

    def upload_file(self, file_name, base64_string, file_type):
        values = {
            "fileBase64String": f"i,{base64_string}",
            "fileName": file_name,
            "fileType": file_type,
        }
        response = self.request_session.post(f"{self.host}/upload_file", json=values, headers=self.headers)
        if response.status_code != 200:
            raise ValueError(f"Failed uploading {response.status_code} {response.text}")

        return response.json()["data_id"]

    def queue_job(self, job_name, data_id, extra_arguments=None):
        values = {"jobName": job_name, "dataId": data_id}
        if extra_arguments is not None:
            values["extraArguments"] = json.dumps(extra_arguments)

        r = self.request_session.post(f"{self.host}/queue_job", json=values, headers=self.headers)
        if r.status_code != 200:
            raise ValueError(f"Failed queuing job {r.status_code} {r.text}")

        return r.json()["job_id"]

    def wait_for_jobs_to_complete(self, job_ids, time_limit=10):
        start_time = time.time()
        while True:
            jobs = self.get_jobs(job_ids, ["Running", "Queued"])
            if len(jobs) == 0 or time.time() - start_time >= time_limit:
                break
            time.sleep(1)

    def print_graph(self, file_name):
        values = {"file_name": file_name}
        response = self.request_session.post(f"{self.host}/print_graph", json=values, headers=self.headers)
        if response.status_code != 200:
            raise ValueError(f"Failed graph print {response.status_code} {response.text}")
        return response

    def empty_database(self):
        response = self.request_session.delete(f"{self.host}/delete_all_data", headers=self.headers)
        if response.status_code != 200:
            raise ValueError(f"Failed delete db {response.status_code} {response.text}")
        return response

    def get_jobs(self, jobs, statuses=None):
        query = """
            query GetJobs($jobIds: [Int!], $statuses: [String!]) {
                jobs(jobIds: $jobIds, statuses: $statuses) {
                    id
                    status
                }
            }
        """
        variables = {
            "jobIds": jobs,  # replace with the actual job IDs
            "statuses": statuses,  # replace with the desired statuses
        }

        response = self.request_session.post(
            f"{self.host}/graphql",
            headers=self.headers,
            json={"query": query, "variables": variables},
        )

        if response.status_code != 200:
            raise ValueError(f"Failed get jobs {response.status_code} {response.text}")
        data = response.json()["data"]
        jobs = data.get("jobs")
        return jobs

    def get_tagged_data(self, filters=[], label_names=None, job_names=None, sort_by=None, order_by=None):
        query = """
            query GetFinalTable($filters: [Compare!], $labelNames: [String!], $jobNames: [String!], $sortBy: String, $orderBy: OrderBy) {
                getFinalTable(filters: $filters, labelNames: $labelNames, jobNames: $jobNames, sortBy: $sortBy, orderBy: $orderBy) {
                    parentDataFileName
                    parentDataCollectionName
                    labelName
                    jobName
                    count
                    columns {
                      dataValue
                      tagName
                      dataType
                    }
                }
            }
        """
        variables = {
            "filters": filters,
            "labelNames": label_names,
            "jobNames": job_names,
            "sortBy": sort_by,
            "orderBy": order_by,
        }

        response = self.request_session.post(
            f"{self.host}/graphql",
            headers=self.headers,
            json={"query": query, "variables": variables},
        )

        if response.status_code != 200:
            raise ValueError(f"Failed get jobs {response.status_code} {response.text}")

        data = response.json()["data"]
        return data["getFinalTable"]

    def get_csv_data(self, filters=[], label_names=None, job_names=None, sort_by=None, order_by=None):
        query = """
            query getCombinedTables($filters: [Compare!], $labelNames: [String!], $jobNames: [String!], $sortBy: String, $orderBy: OrderBy) {
                getCombinedTables(filters: $filters, labelNames: $labelNames, jobNames: $jobNames, sortBy: $sortBy, orderBy: $orderBy)
            }
        """
        variables = {
            "filters": filters,
            "labelNames": label_names,
            "jobNames": job_names,
            "sortBy": sort_by,
            "orderBy": order_by,
        }

        response = self.request_session.post(
            f"{self.host}/graphql",
            headers=self.headers,
            json={"query": query, "variables": variables},
        )

        if response.status_code != 200:
            raise ValueError(f"Failed get jobs {response.status_code} {response.text}")

        data = response.json()["data"]
        return data["getCombinedTables"]

    def semantic_search(self, label_name, search, num_results):
        query = """
            query SemanticSearch($labelName: String!, $search: String!, $numResults: Int) {
              semanticSearch(labelName: $labelName, search: $search, numResults: $numResults) {
                similarityScore
                data {
                  id
                  name
                }
              }
            }
        """
        variables = {
            "labelName": label_name,
            "search": search,
            "numResults": num_results,
        }

        response = self.request_session.post(
            f"{self.host}/graphql",
            headers=self.headers,
            json={"query": query, "variables": variables},
        )

        if response.status_code != 200:
            raise ValueError(f"Failed get jobs {response.status_code} {response.text}")

        data = response.json()["data"]
        return data["semanticSearch"]
