import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any

DB_PATH = Path("data/db/metadata.db")


def get_db_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.row_factory = sqlite3.Row
    return connection


def init_db():
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS documents(doc_id TEXT PRIMARY KEY, filename TEXT NOT NULL, filepath TEXT NOT NULL, filetype TEXT NOT NULL, title TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    cur.execute(
        "CREATE TABLE  IF NOT EXISTS chunks(chunk_id TEXT PRIMARY KEY, doc_id TEXT NOT NULL, page_number INTEGER, native_text TEXT, ocr_text TEXT, chunk_text TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (doc_id) REFERENCES documents(doc_id))")
    cur.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(chunk_id, doc_id, title, chunk_text)")

    connection.commit()
    connection.close()


def upsert_document(doc_id, filename, filepath, filetype, title):
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute("""
    INSERT INTO documents (
        doc_id,
        filename,
        filepath,
        filetype,
        title
    )
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(doc_id) DO UPDATE SET
        filename = excluded.filename,
        filepath = excluded.filepath,
        filetype = excluded.filetype,
        title = excluded.title;
    """, (
        doc_id,
        filename,
        filepath,
        filetype,
        title
    ))
    connection.commit()
    connection.close()


def upsert_chunk(chunk_id, doc_id, page_number, native_text, ocr_text, chunk_text, title):
    connection = get_db_connection()
    cur = connection.cursor()
    cur.execute("""
    INSERT INTO chunks(
        chunk_id,
        doc_id,
        page_number,
        native_text,
        ocr_text,
        chunk_text
    )
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(chunk_id) DO UPDATE SET
        native_text = excluded.native_text,
        ocr_text = excluded.ocr_text,
        chunk_text = excluded.chunk_text;
    """, (
        chunk_id,
        doc_id,
        page_number,
        native_text,
        ocr_text,
        chunk_text
    ))

    cur.execute(
        "DELETE FROM chunks_fts WHERE chunk_id = ?",
        (chunk_id,)
    )

    cur.execute("""
    INSERT INTO chunks_fts (
        chunk_id, doc_id, title, chunk_text
    )
    VALUES (?, ?, ?, ?)
    """, (
        chunk_id, doc_id, title, chunk_text
    ))

    connection.commit()
    connection.close()


def _fts_safe_query(q: str) -> str:
    q = q.strip()
    if (q.startswith('"') and q.endswith('"')) or (q.startswith("'") and q.endswith("'")):
        return q
    if any(ch in q for ch in ['-', ':', '/', '.', '#']):
        q = q.replace('"', '""')
        return f'"{q}"'
    return q


def fts_search(query, limit=10):
    connection = get_db_connection()
    cur = connection.cursor()

    query = _fts_safe_query(query)

    sql = """
    SELECT
        chunk_id,
        doc_id,
        snippet(chunks_fts, 3, '[', ']', '...', 36) AS snippet
    FROM chunks_fts
    WHERE chunks_fts MATCH ?
    ORDER BY bm25(chunks_fts)
    LIMIT ?;
    """

    rows = cur.execute(sql, (query, limit)).fetchall()
    connection.close()
    return [dict(r) for r in rows]


def get_doc(doc_id):
    connection = get_db_connection()
    cur = connection.cursor()

    cur.execute(
        "SELECT * FROM documents WHERE doc_id = ?", (doc_id,)
    )

    row = cur.fetchone()
    connection.close()
    return dict(row) if row else None


def get_chunk(chunk_id):
    connection = get_db_connection()
    cur = connection.cursor()

    cur.execute(
        "SELECT * FROM chunks WHERE chunk_id = ?", (chunk_id,)
    )

    row = cur.fetchone()
    connection.close()
    return dict(row) if row else None
