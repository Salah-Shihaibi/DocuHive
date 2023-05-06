import strawberry
from typing import List, Optional, NewType
from enum import Enum
from DocuHive.graphql_routes.data_jobs_schema import Data


DataValue = strawberry.scalar(
    NewType("JSON", object),
    description="The `JSON` scalar type represents JSON values as specified by ECMA-404",
    serialize=lambda v: v,
    parse_value=lambda v: v,
)


@strawberry.type
class TagData:
    tag_name: str
    data_type: str
    label_name: Optional[str]
    data: Optional[Data] = None
    data_value: Optional[DataValue]


@strawberry.type
class RowData:
    doc: Data
    doc_id: int
    parent_data_file_name: str
    parent_data_collection_name: Optional[str] = None
    job_name: str
    label_name: str
    count: int
    columns: List[TagData]


@strawberry.type
class WorkflowInfo:
    name: str
    debug_options: List[str]


@strawberry.type
class TagInfo:
    data_type: str
    name: str


@strawberry.type
class LabelInfo:
    name: str
    tags: List[TagInfo]
    workflows: List[WorkflowInfo]


@strawberry.enum
class OrderBy(Enum):
    ASC = "ASC"
    DESC = "DESC"


@strawberry.type
class SemanticResult:
    similarity_score: float
    data: Data
