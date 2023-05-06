import os
import pickle
import re
from typing import List, Tuple
import numpy as np
import torch
from sentence_transformers import SentenceTransformer, util
from DocuHive.database.models import DataDB

torch.set_num_threads(1)
script_dir = os.path.dirname(os.path.abspath(__file__))
st = SentenceTransformer("all-mpnet-base-v2", cache_folder=f"{script_dir}/sematic_search_modules")


def semantic_search_similarity(
    search: str,
    overall_text_docs: List[Tuple[str, str]],
    num_results: int = None,
) -> List[Tuple[float, str]]:
    model = SentenceTransformer("all-MiniLM-L6-v2")
    semantic_vector_datas = [overall_text[1] for overall_text in overall_text_docs]
    emb1 = model.encode(search, show_progress_bar=False)
    emb2 = model.encode(semantic_vector_datas, show_progress_bar=False)
    docs = [overall_text[0] for overall_text in overall_text_docs]
    results = sorted(zip(util.cos_sim(emb1, emb2)[0].tolist(), docs), key=lambda x: x[0], reverse=True)
    return results[:num_results]


def semantic_search_similarity_ready_vectors(
    search: str,
    overall_text_docs: List[DataDB],
    num_results: int = None,
) -> List[Tuple[float, DataDB]]:
    if num_results is None:
        num_results = len(overall_text_docs)
    search_vec = st.encode(search, show_progress_bar=False)
    semantic_vectors = np.vstack([np.array(pickle.loads(data.blob)) for data in overall_text_docs])
    data = [data for data in overall_text_docs]
    results = sorted(
        zip(util.cos_sim(search_vec, semantic_vectors)[0].tolist(), data), key=lambda x: x[0], reverse=True
    )
    return results[:num_results]


def prep_semantic_search_vector(semantic_search_text):
    lowercase_text = semantic_search_text.lower()
    cleaned_text = re.sub(r"[^\w\s\-\.\,\—]", " ", lowercase_text)
    vec = st.encode(cleaned_text, show_progress_bar=False)
    return vec
