from langchain_text_splitters import RecursiveCharacterTextSplitter


class LangchainChunker:
    def __init__(
        self,
        chunk_size: int = 1024,
        overlap: int = 50,
        separators: list[str] | None = None,
    ):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.separators = separators or ["\n\n", "\n", ". ", " ", ""]

        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.overlap,
            separators=self.separators,
        )

    def chunk_with_metadata(self, text: str, doc_id: str, source_path: str) -> list[dict]:
        chunks = self.splitter.split_text(text)
        results = []

        for idx, chunk in enumerate(chunks):
            chunk = chunk.strip()
            if not chunk:
                continue

            results.append({
                "text": chunk,
                "metadata": {
                    "doc_id": doc_id,
                    "chunk_index": idx,
                    "source_path": source_path
                }
            })

        return results
