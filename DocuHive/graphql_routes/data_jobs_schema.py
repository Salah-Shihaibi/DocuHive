import strawberry
from typing import List, Optional
from enum import Enum

from sqlalchemy import desc

from DocuHive.backend_utils.utils import get_data_id_of_document
from DocuHive.database.models import DataDB, JobDB, DataType
from sqlalchemy.orm import joinedload, load_only


@strawberry.type
class Job:
    id: str

    @strawberry.field
    def datas(self) -> List["Data"]:
        job = JobDB.query.options(load_only(JobDB.id), joinedload(JobDB.datas).load_only(DataDB.id)).get(self.id)
        return [Data(id=str(data.id)) for data in job.datas]

    @strawberry.field
    def job_name(self) -> str:
        job = JobDB.query.get(self.id)
        return job.workflow.name

    @strawberry.field
    def status(self) -> str:
        job = JobDB.query.options(load_only(JobDB.id, JobDB.status)).get(self.id)
        return job.status

    @strawberry.field
    def identifier(self) -> str:
        job = JobDB.query.options(load_only(JobDB.id, JobDB.identifier)).get(self.id)
        return job.identifier

    @strawberry.field
    def parent_document(self) -> Optional["Data"]:
        doc_id = get_data_id_of_document(job_id=self.id)
        doc_data = DataDB.query.get(doc_id)
        return doc_data


def extend_enum(inherited_enum):
    def wrapper(added_enum):
        joined = {}
        for item in inherited_enum:
            joined[item.name] = item.value
        for item in added_enum:
            joined[item.name] = item.value
        return Enum(added_enum.__name__, joined)

    return wrapper


@strawberry.enum
@extend_enum(DataType)
class DataTypeGQL(Enum):
    pass


@strawberry.type
class Data:
    id: str

    @strawberry.field
    def image_base64_string(self) -> Optional[str]:
        data = DataDB.query.get(int(self.id))
        return data.image_blob

    @strawberry.field
    def pdf_base64_string(self) -> Optional[str]:
        data = DataDB.query.get(int(self.id))
        return data.pdf_blob

    @strawberry.field
    def name(self) -> Optional[str]:
        data = DataDB.query.get(int(self.id))
        return data.name

    @strawberry.field
    def polygons(self) -> Optional[str]:
        data = DataDB.query.options(load_only(DataDB.id, DataDB.polygons)).get(int(self.id))
        return data.polygons

    @strawberry.field
    def jobs(self, job_name: Optional[str] = None) -> List["Job"]:
        data = DataDB.query.options(
            load_only(DataDB.id),
            joinedload(DataDB.jobs),
        ).get(int(self.id))
        jobs = [Job(id=int(job.id)) for job in data.jobs if job_name is None or job.workflow.name == job_name]
        return jobs

    @strawberry.field
    def data_type(self) -> DataTypeGQL:
        data = DataDB.query.options(load_only(DataDB.id, DataDB.data_type)).get(int(self.id))
        return DataTypeGQL[data.data_type.name]

    @strawberry.field
    def doc_page_dimensions(self) -> Optional[str]:
        data = DataDB.query.options(load_only(DataDB.id, DataDB.page_dimensions)).get(int(self.id))
        return data.page_dimensions


@strawberry.type
class DataJobQuery:
    @strawberry.field
    def data(self, data_id: int) -> Optional[Data]:
        data = DataDB.query.get(int(data_id))
        return None if data is None else Data(id=data_id)

    @strawberry.field
    def datas(
        self,
        data_type: Optional[DataTypeGQL] = None,
        data_ids: Optional[List[int]] = None,
    ) -> List[Data]:
        datas = DataDB.query
        if data_type is not None:
            datas = datas.filter(DataDB.data_type == DataType[data_type.name])
        if data_ids is not None:
            datas = datas.filter(DataDB.id.in_(tuple(data_ids)))
        datas = datas.options(load_only(DataDB.id)).order_by(desc(DataDB.id)).all()
        return [Data(id=data.id) for data in datas]

    @strawberry.field
    def jobs(self, statuses: Optional[List[str]] = None, job_ids: Optional[List[int]] = None) -> List[Job]:
        jobs = JobDB.query
        if statuses is not None:
            jobs = jobs.filter(JobDB.status.in_(statuses))
        if job_ids is not None:
            jobs = jobs.filter(JobDB.id.in_(tuple(job_ids)) | JobDB.identifier.in_(tuple(str(job_ids))))
        jobs = jobs.options(load_only(JobDB.id)).all()
        return [Job(id=job.id) for job in jobs]


@strawberry.type
class DataMutation:
    @strawberry.mutation
    def hello(self) -> str:
        return "hello"
