import json
import re
from DocuHive.database.models import (
    DataDB,
    JobDB,
    data_job_table,
    job_data_table,
    DataTagDataDB,
    TagDB,
)
from sqlalchemy import select
from graphviz import Digraph

from DocuHive.database.setup import db


class TreeGraph:
    def __init__(self):
        dot = Digraph(strict=True)
        dot.attr(rankdir="LR", ranksep="1.5", nodesep="0.2")
        self.dot = dot
        self.fontsize = "30"
        self.height = "0.3"
        self.width = "0.7"
        self.datas = None
        self.jobs = None
        self.root_datas = []
        self.root_jobs = []

    def update_label(self, node_id, new_data):
        match = None
        for i, node in enumerate(self.dot.body):
            if re.search(f"{node_id} \[label=", node):
                match = node
                break
        if match is None:
            raise Exception(f"no node with id {node_id}")

        label = json.loads(re.search(r'label="({[^}]+})"', match).group(1).replace("\\", ""))
        for key, value in new_data.items():
            label[key] = value
        x = json.dumps(label, indent=4).replace('"', '\\"')
        new_label = f'label="{x}"'
        self.dot.body[i] = re.sub(r'label="({[^}]+})"', new_label, match)

    def get_data_info(self, data_row):
        data = {
            "title": "Data",
            "id": data_row.id,
            "data_type": data_row.data_type.name,
        }
        if data_row.data_type.name in ["file", "collection"]:
            data["name"] = data_row.name
        else:
            data["value"] = str(getattr(data_row, data_row.data_type.name))[:100]
            data["data_type"] = data_row.data_type.name
            # data["polygons"] = data_row.polygon_relative_to_parent
        return json.dumps(data, indent=4)

    def get_job_info(self, job_row):
        job = {
            "title": "Job",
            "id": job_row.id,
            "name": job_row.workflow.name,
            "identifier": job_row.identifier,
            "status": job_row.status,
            "time_created": str(job_row.time_created),
            "time_updated": str(job_row.time_updated),
            "label": job_row.workflow.label.name
            # "task_id": job_row.task_id,
        }
        return json.dumps(job, indent=4)

    def get_root_data(self):
        datas = []
        jobs = []

        all_data = db.session.query(DataDB).all()
        for data in all_data:
            if len(data.jobs_reverse) == 0:
                datas.append(data)

        all_jobs = db.session.query(JobDB).all()
        for job in all_jobs:
            if len(job.datas_reversed) == 0:
                jobs.append(job)

        self.root_datas = datas
        self.root_jobs = jobs

    def add_job_to_data_node(self, job, data=None):
        job_info = self.get_job_info(job)
        job_id = "job" + str(job.id)
        self.dot.node(
            job_id,
            width=self.width,
            height=self.height,
            fontsize=self.fontsize,
            label=job_info,
            shape="oval",
        )
        if data:
            data_id = "data" + str(data.id)
            self.dot.edge(data_id, job_id)

    def add_data_to_job_node(self, data, job=None):
        data_info = self.get_data_info(data)
        data_id = "data" + str(data.id)
        self.dot.node(
            data_id,
            width=self.width,
            height=self.height,
            fontsize=self.fontsize,
            label=data_info,
            shape="box",
        )
        if job:
            job_id = "job" + str(job.id)
            self.dot.edge(job_id, data_id)

    def link_datas_jobs(self, job_data):
        if job_data.__tablename__ == "data_db":
            data = job_data
            jobs = data.jobs
            for job in jobs:
                if self.jobs is None or job.id in self.job:
                    self.add_job_to_data_node(job=job, data=data)
                    self.link_datas_jobs(job)
        elif job_data.__tablename__ == "job_db":
            job = job_data
            datas = job.datas
            for data in datas:
                if self.datas is None or data.id in self.datas:
                    self.add_data_to_job_node(job=job, data=data)
                    self.link_datas_jobs(data)
        else:
            raise Exception("Provide data_job_toggle")
        return

    def build_graph_from_join_table(self):
        query = select(data_job_table)
        data_job = db.session.execute(query).fetchall()
        for data_job_info in data_job:
            data_id, job_id = data_job_info
            if (self.datas is None or data_id in self.datas) and (self.job is None or job_id in self.job_id):
                self.add_job_to_data_node(data=DataDB.query.get(data_id), job=JobDB.query.get(job_id))

        query = select(job_data_table)
        job_data = db.session.execute(query).fetchall()
        for job_data_info in job_data:
            job_id, data_id = job_data_info
            if (self.datas is None or data_id in self.datas) and (self.job is None or job_id in self.job_id):
                self.add_data_to_job_node(
                    data=DataDB.query.get(data_id),
                    job=JobDB.query.get(job_id),
                )

    def get_tag_nodes(self):
        tags = db.session.query(TagDB).all()
        for tag in tags:
            tag_info = tag.name
            tag_id = tag.name
            self.dot.node(
                tag_id,
                width=self.width,
                height=self.height,
                fontsize=self.fontsize,
                label=tag_info,
                shape="diamond",
            )

    def link_tags_to_data_and_labels_to_tags(self):
        all_tagged_data = db.session.query(DataTagDataDB).all()
        for tagged_data in all_tagged_data:
            tag_id = tagged_data.tag.name
            data_id = "data" + str(tagged_data.data_id)
            self.dot.edge(data_id, tag_id)

    def build_graph_using_dfs(self):
        for data in self.root_datas:
            self.add_data_to_job_node(data=data)
            self.link_datas_jobs(data)

        for job in self.root_jobs:
            self.add_job_to_data_node(job=job)
            self.link_datas_jobs(job)

    def read_tree_json(self):
        shape_map = {"folder": "file", "box": "data", "oval": "job"}
        with open("tree_diagram.json", "r") as graph_data:
            graph_data = json.load(graph_data)
            nodes = []
            edges = []
            limits = {"x_min": 1000000, "x_max": 0, "y_min": 1000000, "y_max": 0}
            for node in graph_data["objects"]:
                if "label" in node and "pos" in node:
                    try:
                        label = json.loads(node["label"].replace("\\", ""))
                    except:
                        label = node["label"].replace("\\", "")
                    nodes.append(
                        {
                            "num": node["_gvid"],
                            "name": node["name"],
                            "type": shape_map[node["shape"]],
                            "label": label,
                            "pos": node["pos"].split(","),
                        }
                    )
                    x, y = node["pos"].split(",")
                    if float(x) < limits["x_min"]:
                        limits["x_min"] = float(x)
                    if float(x) > limits["x_max"]:
                        limits["x_max"] = float(x)
                    if float(y) < limits["y_min"]:
                        limits["y_min"] = float(y)
                    if float(y) > limits["y_max"]:
                        limits["y_max"] = float(y)

            for edge in graph_data["edges"]:
                head = nodes[edge["head"] - 1]["name"]
                tail = nodes[edge["tail"] - 1]["name"]
                edges.append({"head": head, "tail": tail})
        return {"limits": limits, "nodes": nodes, "edges": edges}

    def get_filtered_data(self):
        self.root_datas = []  # parent_id
        self.datas = []  # data_id
        self.jobs = []  # reverse_jobs

    def seed_graph(self, pdf_name):
        self.get_root_data()
        self.build_graph_using_dfs()
        # TG.build_graph_from_join_table(dot)
        self.get_tag_nodes()
        self.link_tags_to_data_and_labels_to_tags()
        self.dot.render(pdf_name, format="pdf")
