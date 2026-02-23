from collections import defaultdict
from typing import List, Dict


def search_files(vector_store, query: str, top_k_chunks: int = 10):
    chunk_hits = vector_store.search(query, top_k=top_k_chunks)

    files = {}

    for hit in chunk_hits:
        source_path = hit.get("source_path")
        score = hit.get("score", 0.0)

        if not source_path:
            continue

        if source_path not in files:
            files[source_path] = {
                "source_path": source_path,
                "score": score,
                "best_chunk": hit.get("text", ""),
            }
        else:
            if score > files[source_path]["score"]:
                files[source_path]["score"] = score
                files[source_path]["best_chunk"] = hit.get("text", "")

    results = sorted(
        files.values(),
        key=lambda x: x["score"],
        reverse=True,
    )

    return results
