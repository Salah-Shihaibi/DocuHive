from datetime import datetime
import pandas as pd
import strawberry
from sqlalchemy import text
from sqlalchemy.exc import NoResultFound

from DocuHive.backend_utils.utils import (
    replace_special_chars,
    get_special_chars,
)
from DocuHive.data_science.doc_data_extraction.semantic_search import (
    semantic_search_similarity_ready_vectors,
)
from DocuHive.database.models import LabelDB, TagDB, DataDB, DataTagDataDB
from typing import List, Optional

from DocuHive.database.setup import db
from DocuHive.graphql_routes.data_jobs_schema import Data
from DocuHive.graphql_routes.tagged_data_types import (
    RowData,
    TagData,
    DataValue,
    OrderBy,
    TagInfo,
    LabelInfo,
    WorkflowInfo,
    SemanticResult,
)


datatype_operations_and_values = {
    "text": {"operations": ["=", "<>", "Search Keyword"], "values": None},
    "boolean": {"operations": None, "values": ["TRUE", "FALSE"]},
    "float": {"operations": [">", "<", ">=", "<=", "=", "<>"], "values": None},
    "integer": {
        "operations": [">", "<", ">=", "<=", "=", "<>"],
        "values": None,
    },
    "data": {"operations": None, "values": ["found", "not_found"]},
    "date": {
        "operations": [">", "<", ">=", "<=", "=", "<>", "EXTRACT(YEAR FROM TIMESTAMP)"],
        "values": None,
    },
}


def falsy_data_comparisons(compare_info):
    logic1 = ""
    logic2 = ""

    if compare_info.value == "found":
        logic1 = "not"
        logic2 = "!"

    res = [
        f"{compare_info.column} is {logic1} NULL",
        f"{compare_info.column} {logic2}= ''",
        f"TRIM({compare_info.column}) {logic2}= ''",
    ]

    return [f"({' OR '.join(res)})"]


def string_data_comparison(compare_info):
    if compare_info.operation != "Search Keyword":
        return f"{compare_info.column} {compare_info.operation} '{compare_info.value}'"
    else:
        return f"LOWER({compare_info.column}) LIKE LOWER('%{compare_info.value}%')"


def numeric_data_comparison(compare_info):
    return f"{compare_info.column} {compare_info.operation} {compare_info.value}"


def boolean_data_comparison(compare_info):
    # SQLite expects a 1 or 0 when comparing booleans
    map_bool = {"True": 1, "False": 0}
    return f"{compare_info.column} = {map_bool[compare_info.value]}"


def date_data_comparison(compare_info):
    if compare_info.operation == "year":
        return f"{compare_info.column} REGEXP '^[0-9]{{2}} [A-Za-z]{{3}}$'"

    try:
        date_format = "%Y-%m-%d"
        datetime.strptime(compare_info.value, date_format)
    except:
        raise ValueError(f"{compare_info.value} is an invalid date")

    return f"{compare_info.column} {compare_info.operation} '{compare_info.value}'"


def add_comparison_sql(compare_info, sort_filter_toggle=None):
    res = []
    if compare_info.data_type == "data":
        res_list = falsy_data_comparisons(compare_info)
        for result in res_list:
            res.append(result)

    elif compare_info.data_type == "text":
        res.append(string_data_comparison(compare_info))

    elif compare_info.data_type in ["float", "integer"]:
        res.append(numeric_data_comparison(compare_info))

    elif compare_info.data_type == "boolean":
        res.append(boolean_data_comparison(compare_info))

    elif compare_info.data_type == "date":
        res.append(date_data_comparison(compare_info))

    return res


def datatype_comparison(compare_type, tag_name):
    # any tag can be checked for falsy values
    if compare_type == "data":
        return True

    tag = TagDB.query.filter_by(name=tag_name).first()
    if tag.data_type.name != compare_type:
        raise ValueError(f"{tag_name} is of type {tag.data_type.name} not {compare_type}")
    return True


def check_values_and_operations(compare_info):
    values = datatype_operations_and_values[compare_info.data_type]["values"]
    operations = datatype_operations_and_values[compare_info.data_type]["operations"]

    if operations is not None:
        if compare_info.operation not in operations:
            raise ValueError(
                f"the operation {compare_info.operation} is not one of the following {compare_info.data_type} operations: {operations}"
            )
    if values is not None:
        if compare_info.value not in values:
            raise ValueError(
                f"the value {compare_info.value} is not one of the following {compare_info.data_type} values: {values}"
            )
    return True


def sorting_and_filtering(compare_list, unique_tags, sort_filter_toggle):
    if len(compare_list) > 0:
        sql_comparisons_list = []
        for compare_info in compare_list:
            if compare_info.column not in unique_tags:
                raise NoResultFound("The column you are trying to sort by is not found")

            if datatype_comparison(compare_info.data_type, compare_info.column) and check_values_and_operations(
                compare_info
            ):
                compare_info.column = replace_special_chars(compare_info.column)
                sql_comparisons_list += add_comparison_sql(compare_info, sort_filter_toggle)

        if sort_filter_toggle == "sort":
            order_sql_table = "ORDER BY CASE"
            for i, sql_comparison in enumerate(sql_comparisons_list):
                order_sql_table += f" WHEN {sql_comparison} THEN {i + 1}"

            order_sql_table += f" ELSE {len(sql_comparisons_list) + 1}" f" END"
            return order_sql_table

        elif sort_filter_toggle == "filter":
            filter_sql_table = f" WHERE {sql_comparisons_list[0]}"
            for i, sql_comparison in enumerate(sql_comparisons_list[1:]):
                filter_sql_table += f" AND {sql_comparison}"
            return filter_sql_table

    return ""


def format_date_data(data_type):
    if data_type == "date":
        return (
            f" CASE"
            f" WHEN strftime('%Y', data_db_1.{data_type}) = '0001'"
            f" THEN strftime('%d', data_db_1.{data_type}) || ' ' || substr('JanFebMarAprMayJunJulAugSepOctNovDec', 1 + 3*strftime('%m', data_db_1.{data_type}), -3)"
            f" ELSE CONCAT(data_db_1.{data_type}, '$%$', data_db_1.polygon) "
            f" END "
        )
    return f"data_db_1.{data_type} || '$%$' || data_db_1.polygon"


def get_tagged_data_query_info(filters, job_names, label_names, order_by, sort_by, data_id_required=False):
    if label_names is None:
        labels = LabelDB.query.all()
    else:
        labels = LabelDB.query.filter(LabelDB.name.in_(tuple(label_names)))
        if labels is None:
            raise ValueError("Label not found")
    unique_tag_names = []
    unique_tags = []
    for label in labels:
        for tag in label.tags:
            if tag not in unique_tag_names and tag.data_type.name in [
                "text",
                "integer",
                "float",
                "boolean",
                "date",
            ]:
                unique_tag_names.append(tag.name)
                unique_tags.append({"name": tag.name, "data_type": tag.data_type.name})
    columns = []
    for tag in unique_tags:
        column_name = replace_special_chars(tag["name"])
        column = f"MAX(CASE WHEN tag_db.name = '{tag['name']}' THEN data_db_1.{tag['data_type']} END) AS {column_name}"
        columns.append(column)
        if data_id_required:
            column = f"MAX(CASE WHEN tag_db.name = '{tag['name']}' THEN data_db_1.id END) AS {column_name}_id"
            columns.append(column)
    label_job_filter = ""
    if label_names is not None:
        label_job_filter += " AND label_db.name IN (" + ", ".join("'{}'".format(val) for val in label_names) + ") "
    if job_names is not None:
        label_job_filter += (
            " AND data_tag_data_db.job_name IN (" + ", ".join("'{}'".format(val) for val in job_names) + ") "
        )
    filter_table_sql = sorting_and_filtering(
        compare_list=filters,
        unique_tags=unique_tag_names,
        sort_filter_toggle="filter",
    )
    order_table_sql = ""
    if sort_by is not None:
        if sort_by not in unique_tag_names and sort_by != "File_name":
            raise ValueError(f"column '{sort_by}' not found in table '{unique_tag_names}' columns")
        sort_by = replace_special_chars(sort_by)
        order_table_sql = f"ORDER BY {sort_by} {order_by.name}"

    return columns, unique_tags, label_job_filter, filter_table_sql, order_table_sql


def get_columns(row, columns, tags):
    tagged_data = []
    for x, i in enumerate(range(0, len(columns), 2)):
        val, data_id = columns[i : i + 2]
        data = Data(id=data_id) if data_id else None
        tagged_data.append(
            TagData(
                tag_name=tags[x]["name"],
                data_type=tags[x]["data_type"],
                data_value=val,
                data=data,
                label_name=row.label_name,
            )
        )
    return tagged_data


def get_tagged_data_sql_query(
    filters,
    job_names,
    label_names,
    order_by,
    sort_by,
    required_fields=None,
    data_id_required=False,
):
    (
        columns,
        unique_tags,
        label_job_filter,
        filter_table_sql,
        order_table_sql,
    ) = get_tagged_data_query_info(filters, job_names, label_names, order_by, sort_by, data_id_required)
    if required_fields is None:
        required_fields = f"data_db.id AS doc_id, data_db.name AS file_name, data_db_2.name AS collection_name, label_db.name AS label_name, data_tag_data_db.job_name AS job_name, COUNT(data_tag_data_db.tag_id) AS count"

    pivot_query = (
        f" SELECT {required_fields}, {', '.join(columns)}"
        f" FROM data_tag_data_db"
        f" LEFT JOIN tag_db ON tag_db.id = data_tag_data_db.tag_id"
        f" LEFT JOIN label_db ON label_db.id = data_tag_data_db.label_id"
        f" LEFT JOIN data_db ON data_db.id = data_tag_data_db.parent_file_id"
        f" LEFT JOIN data_db AS data_db_1 ON data_db_1.id = data_tag_data_db.data_id"
        f" LEFT JOIN data_db AS data_db_2 ON data_db_2.id = data_tag_data_db.parent_collection_id"
        f" WHERE 1 = 1 {label_job_filter}"
        f" GROUP BY doc_id, file_name, collection_name, label_name, job_name"
        f" {order_table_sql}"
    )

    if filter_table_sql != "":
        pivot_query = f" WITH cte_name AS ({pivot_query})" f" SELECT * FROM cte_name" f" {filter_table_sql}"

    results = db.session.execute(text(pivot_query)).all()

    return results, unique_tags


@strawberry.input
class Compare:
    column: str
    data_type: str
    operation: Optional[str] = None
    value: Optional[DataValue] = None


@strawberry.type
class TaggedDataQueries:
    @strawberry.field
    def get_final_table(
        self,
        filters: Optional[List[Compare]] = [],
        label_names: Optional[List[str]] = None,
        job_names: Optional[List[str]] = None,
        sort_by: Optional[str] = None,
        order_by: Optional[OrderBy] = OrderBy.ASC,
    ) -> List[RowData]:
        results, unique_tags = get_tagged_data_sql_query(
            filters, job_names, label_names, order_by, sort_by, data_id_required=True
        )
        return [
            RowData(
                doc=Data(id=row.doc_id),
                doc_id=row.doc_id,
                parent_data_file_name=row.file_name,
                parent_data_collection_name=row.collection_name,
                job_name=row.job_name,
                label_name=row.label_name,
                count=row.count,
                columns=get_columns(row, row[6:], tags=unique_tags),
            )
            for row in results
        ]

    @strawberry.field
    def get_combined_tables(
        self,
        filters: Optional[List[Compare]] = [],
        label_names: Optional[List[str]] = None,
        job_names: Optional[List[str]] = None,
        sort_by: Optional[str] = None,
        order_by: Optional[OrderBy] = OrderBy.ASC,
    ) -> str:
        required_fields = f"data_db.id AS doc_id, data_db.name AS file_name, data_db_2.name AS collection_name, label_db.name AS label_name, data_tag_data_db.job_name AS job_name"
        results, unique_tags = get_tagged_data_sql_query(
            filters,
            job_names,
            label_names,
            order_by,
            sort_by,
            required_fields=required_fields,
        )
        df = pd.DataFrame(results)
        for col in df.columns:
            df.rename(columns={col: get_special_chars(col)}, inplace=True)

        return df.to_csv()

    @strawberry.field
    def get_label_tags(self, label_names: Optional[List[str]] = None) -> List[TagInfo]:
        if label_names is None:
            labels = LabelDB.query.all()
        else:
            labels = LabelDB.query.filter(LabelDB.name.in_(tuple(label_names)))
            if labels is None:
                raise ValueError("Label not found")

        unique_tag_names = []
        tags = []
        for label in labels:
            for tag in label.tags:
                unique_tag_names.append(tag.name)
                tags += [TagInfo(name=tag.name, data_type=tag.data_type.name)]
        return tags

    @strawberry.field
    def get_label_names(self) -> List[str]:
        labels = LabelDB.query.all()
        return [label.name for label in labels]

    @strawberry.field
    def get_labels_data(self, label_names: Optional[List[str]] = None) -> List[LabelInfo]:
        if label_names is None:
            labels = LabelDB.query.all()
        else:
            labels = LabelDB.query.filter(LabelDB.name.in_(tuple(label_names)))
            if labels is None:
                raise ValueError("Label not found")

        labels_data = []
        for label in labels:
            tags = []
            for tag in label.tags:
                tags.append(TagInfo(name=tag.name, data_type=tag.data_type.name))

            workflows = []
            for workflow in label.workflows:
                workflows.append(
                    WorkflowInfo(
                        name=workflow.name,
                        debug_options=workflow.debug_options.split("$:$"),
                    )
                )

            labels_data.append(LabelInfo(name=label.name, tags=tags, workflows=workflows))
        return labels_data

    @strawberry.field
    def semantic_search(self, label_name: str, search: str, num_results: Optional[int] = 25) -> List[SemanticResult]:
        datas = (
            DataDB.query.join(DataTagDataDB, DataDB.id == DataTagDataDB.data_id)
            .join(TagDB, DataTagDataDB.tag_id == TagDB.id)
            .join(LabelDB, DataTagDataDB.label_id == LabelDB.id)
            .filter((TagDB.name == "Semantic Search Blob") & (LabelDB.name == label_name) & DataDB.blob.isnot(None))
            .all()
        )
        if len(datas) == 0:
            raise Exception(f"No data found for the specified criteria. label name = {label_name}")
        sims = semantic_search_similarity_ready_vectors(search, datas, num_results)

        return [
            SemanticResult(
                similarity_score=sim[0],
                data=Data(id=sim[1].data_tag_data[0].parent_file_data.id),
            )
            for sim in sims
        ]
