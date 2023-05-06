import pytest
from DocuHive.tests.DocuHive_client.client import DocuHiveClient
from DocuHive.tests.utils import upload_and_process_docs, save_csv, semantic_search_results


def test_cv_extractor_e2e():
    client = DocuHiveClient()
    job_ids = upload_and_process_docs(client, "generic_cv_extractor", "linkedin_cv")

    client.wait_for_jobs_to_complete(job_ids, 1000)
    tagged_data = client.get_tagged_data()
    csv = client.get_csv_data()
    save_csv(csv)

    search_query = "love business and making money"
    expected_order = ["finance1.pdf", "finance2.pdf", "finance3.pdf"]
    semantic_search_results(client, expected_order, search_query, num_results=3)

    search_query = "love curing people from disease"
    expected_order = ["medicine2.pdf", "medicine1.pdf", "medicine3.pdf"]
    semantic_search_results(client, expected_order, search_query, num_results=3)

    search_query = "I love building computer programs"
    expected_order = ["software3.pdf", "software1.pdf", "software2.pdf"]
    semantic_search_results(client, expected_order, search_query, num_results=3)
    client.print_graph("example")
