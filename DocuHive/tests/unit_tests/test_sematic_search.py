import pytest
from DocuHive.data_science.doc_data_extraction.semantic_search import (
    semantic_search_similarity,
)


def test_semantic_search_similarity():
    documents = [
        ("doc1", "Lions are carnivorous mammals that live in grasslands and savannas."),
        (
            "doc2",
            "Elephants are herbivorous mammals that are known for their large size and long trunks.",
        ),
        ("doc3", "Airplanes are vehicles that can fly in the sky."),
        (
            "doc4",
            "Trains are vehicles that run on tracks and transport goods and people.",
        ),
        ("doc5", "Cats are domesticated mammals that are popular pets."),
    ]

    search_query = "A vehicle moving through the landscape."
    expected_order = ["doc4", "doc3"]
    results = semantic_search_similarity(search_query, documents, num_results=2)
    result_order = [doc_id for _, doc_id in results]
    assert result_order == expected_order, f"Expected order {expected_order}, but got {result_order}"

    search_query = "What are some common behaviors exhibited by mammals in the wild?"
    expected_order = ["doc1", "doc2", "doc5"]
    results = semantic_search_similarity(search_query, documents, num_results=3)
    result_order = [doc_id for _, doc_id in results]
    assert result_order == expected_order, f"Expected order {expected_order}, but got {result_order}"
