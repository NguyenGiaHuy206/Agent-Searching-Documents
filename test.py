from doc_finder_mvp.search.vector_store import VectorStore
from doc_finder_mvp.search.retriever import search_files

vs = VectorStore()

results = search_files(
    vector_store=vs,
    query="support vector machine"
)

for r in results:
    print(r["source_path"], r["score"])
