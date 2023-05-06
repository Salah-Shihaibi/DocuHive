import strawberry
from strawberry.tools import merge_types
from DocuHive.graphql_routes.data_jobs_schema import DataJobQuery, DataMutation
from DocuHive.graphql_routes.tagged_data_schema import TaggedDataQueries

queries = (DataJobQuery, TaggedDataQueries)
mutations = (DataMutation,)

Query = merge_types("Query", queries)
Mutation = merge_types("Mutation", mutations)

schema = strawberry.Schema(query=Query, mutation=Mutation)
