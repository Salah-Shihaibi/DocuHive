import pytest
from DocuHive.tests.DocuHive_client.client import DocuHiveClient
from DocuHive.tests.utils import upload_and_process_docs, save_csv, semantic_search_results


def test_movies_extractor_e2e():
    client = DocuHiveClient()
    job_ids = upload_and_process_docs(client, "movie_extractor", "movies")

    client.wait_for_jobs_to_complete(job_ids, 1000)
    tagged_data = client.get_tagged_data()
    csv = client.get_csv_data()
    save_csv(csv)

    search_query = "story centered around athletes or a team competing in a particular sport."
    expected_order = [
        "Moneyball - Rotten Tomatoes.pdf",
        "Hustle - Rotten Tomatoes.pdf",
        "Big George Foreman_ The Miraculous Story of the Once and Future Heavyweight Champion of the World - Rotten Tomatoes.pdf",
    ]
    semantic_search_results(client, expected_order, search_query, num_results=3, label_name="movie")

    search_query = "thrilling, high-stakes scenes involving physical combat, gunfights, car chases, explosions, and other forms of intense action."
    expected_order = [
        "Fast & Furious 6 - Rotten Tomatoes.pdf",
        "John Wick_ Chapter 4 - Rotten Tomatoes.pdf",
        "The Dark Knight Rises - Rotten Tomatoes.pdf",
    ]
    semantic_search_results(client, expected_order, search_query, num_results=3, label_name="movie")

    search_query = "embarrassing, or awkward situations, often leading to unexpected and hilarious outcomes. witty dialogue, and exaggerated characters."
    expected_order = [
        "Dumb and Dumber To - Rotten Tomatoes.pdf",
        "Superbad - Rotten Tomatoes.pdf",
        "21 Jump Street - Rotten Tomatoes.pdf",
    ]
    semantic_search_results(client, expected_order, search_query, num_results=3, label_name="movie")
    client.print_graph("example")
