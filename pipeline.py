from pathlib import Path
from doc_finder_mvp.ocr.ocr_engine import extract_pdf_to_text
from doc_finder_mvp.ingest.chunking import LangchainChunker
from doc_finder_mvp.search.vector_store import VectorStore

INPUT_DIR = Path("./data/raw_docs")

chunker = LangchainChunker(chunk_size=1024, overlap=50)
vs = VectorStore()

for pdf_path in INPUT_DIR.glob("*.pdf"):
    text = extract_pdf_to_text(pdf_path)

    chunks = chunker.chunk_with_metadata(
        text=text,
        doc_id=pdf_path.stem,
        source_path=str(pdf_path.resolve())
    )
    vs.upsert_chunks(chunks)

    print(f"✅ {pdf_path.stem}: {len(chunks)} chunks ingested")
