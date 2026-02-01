from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

from bs4 import BeautifulSoup

APP_DIR = Path(__file__).resolve().parent
DEFAULT_EXPORT_DIR = APP_DIR.parent / "Basic_LinkedInDataExport_01-31-2026.zip"

EXPORT_DIR = Path(
    os.getenv("LINKEDIN_EXPORT_DIR", str(DEFAULT_EXPORT_DIR))
).expanduser()
DATA_DIR = Path(os.getenv("LINKEDIN_QA_DATA_DIR", str(APP_DIR / "data"))).expanduser()
DB_PATH = Path(
    os.getenv("LINKEDIN_QA_DB", str(DATA_DIR / "linkedin.sqlite"))
).expanduser()

SCHEMA_VERSION = "1"


def ensure_db(
    rebuild: bool = False,
    export_dir: Path | None = None,
    db_path: Path | None = None,
) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    export_dir = (export_dir or EXPORT_DIR).expanduser()
    db_path = (db_path or DB_PATH).expanduser()

    if rebuild and db_path.exists():
        db_path.unlink()

    if not db_path.exists():
        build_database(export_dir=export_dir, db_path=db_path)
        return

    if not _manifest_matches(export_dir=export_dir, db_path=db_path):
        db_path.unlink()
        build_database(export_dir=export_dir, db_path=db_path)


def set_export_dir(path: Path) -> None:
    global EXPORT_DIR
    EXPORT_DIR = path.expanduser().resolve()


def build_database(export_dir: Path | None = None, db_path: Path | None = None) -> None:
    export_dir = (export_dir or EXPORT_DIR).expanduser()
    db_path = (db_path or DB_PATH).expanduser()
    if not export_dir.exists():
        raise FileNotFoundError(f"LinkedIn export not found at {export_dir}")

    conn = sqlite3.connect(db_path)
    try:
        _create_schema(conn)
        signature = _export_signature(export_dir)

        total_docs = 0
        sources: Dict[str, Tuple[int, List[str]]] = {}

        for doc in iter_documents(export_dir):
            source_file, row_id, title, body, data_json, columns = doc
            _insert_document(conn, source_file, row_id, title, body, data_json)
            total_docs += 1

            if source_file not in sources:
                sources[source_file] = (0, columns)
            sources[source_file] = (sources[source_file][0] + 1, columns)

        _insert_sources(conn, sources)
        _set_manifest(conn, signature, total_docs, export_dir)
        conn.commit()
    finally:
        conn.close()


def iter_documents(export_dir: Path) -> Iterator[Tuple[str, int, str, str, str, List[str]]]:
    for path in iter_export_files(export_dir):
        rel_path = path.relative_to(export_dir).as_posix()
        if path.suffix.lower() == ".csv":
            yield from _iter_csv_documents(path, rel_path)
        elif path.suffix.lower() in {".html", ".htm", ".txt"}:
            text = _read_text_file(path)
            if not text:
                continue
            title = _title_from_filename(path.name)
            body = text
            data_json = json.dumps({"text": text}, ensure_ascii=True)
            yield (rel_path, 1, title, body, data_json, ["text"])


def iter_export_files(export_dir: Path) -> List[Path]:
    files: List[Path] = []
    for path in export_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        if path.suffix.lower() in {".csv", ".html", ".htm", ".txt"}:
            files.append(path)
    return sorted(files)


def _iter_csv_documents(path: Path, rel_path: str) -> Iterator[Tuple[str, int, str, str, str, List[str]]]:
    rows = _read_csv_rows(path)
    if rows is None:
        text = _read_text_file(path)
        if not text:
            return
        title = _title_from_filename(path.name)
        data_json = json.dumps({"text": text}, ensure_ascii=True)
        yield (rel_path, 1, title, text, data_json, ["text"])
        return

    row_id = 0
    columns: List[str] = []
    for row in rows:
        if not columns:
            columns = list(row.keys())
        row_id += 1
        cleaned = {k: _clean_text(v) for k, v in row.items()}
        title = _infer_title(cleaned, rel_path, row_id)
        body = _row_to_text(cleaned)
        data_json = json.dumps(cleaned, ensure_ascii=True)
        yield (rel_path, row_id, title, body, data_json, columns)


def _read_csv_rows(path: Path) -> Iterator[Dict[str, str]] | None:
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            header_line = ""
            notes_mode = False
            while True:
                line = handle.readline()
                if not line:
                    return None
                stripped = line.strip()
                if not stripped:
                    notes_mode = False
                    continue
                if stripped.lower().startswith("notes:"):
                    notes_mode = True
                    continue
                if notes_mode:
                    continue
                if "," in stripped:
                    header_line = stripped
                    break

            headers = next(csv.reader([header_line]))
            reader = csv.DictReader(handle, fieldnames=headers)
            for row in reader:
                if not any(v and v.strip() for v in row.values()):
                    continue
                yield row
    except OSError:
        return None


def _read_text_file(path: Path) -> str:
    try:
        raw = path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError:
        return ""

    if path.suffix.lower() in {".html", ".htm"}:
        soup = BeautifulSoup(raw, "html.parser")
        return _clean_text(soup.get_text(" ", strip=True))
    return _clean_text(raw)


def _row_to_text(row: Dict[str, str]) -> str:
    parts: List[str] = []
    for key, value in row.items():
        if not value:
            continue
        parts.append(f"{key}: {value}")
    return "\n".join(parts)


def _infer_title(row: Dict[str, str], source: str, row_id: int) -> str:
    title_fields = [
        "Content Title",
        "Title",
        "Job Title",
        "Company Name",
        "Company",
        "Organization",
        "School Name",
        "First Name",
        "Last Name",
        "Headline",
    ]

    if row.get("First Name") and row.get("Last Name"):
        return f"{row['First Name']} {row['Last Name']}".strip()

    for field in title_fields:
        value = row.get(field)
        if value:
            return value

    return f"{source} row {row_id}"


def _title_from_filename(filename: str) -> str:
    base = re.sub(r"[_\-]+", " ", Path(filename).stem)
    return base.strip().title()


def _clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"\s+", " ", value.strip())
    return text


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY,
            source_file TEXT NOT NULL,
            row_id INTEGER,
            title TEXT,
            body TEXT,
            data_json TEXT
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts
        USING fts5(title, body, content='documents', content_rowid='id');

        CREATE TABLE IF NOT EXISTS sources (
            source_file TEXT PRIMARY KEY,
            row_count INTEGER NOT NULL,
            columns_json TEXT
        );

        CREATE TABLE IF NOT EXISTS manifest (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )


def _insert_document(
    conn: sqlite3.Connection,
    source_file: str,
    row_id: int,
    title: str,
    body: str,
    data_json: str,
) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO documents (source_file, row_id, title, body, data_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (source_file, row_id, title, body, data_json),
    )
    doc_id = cursor.lastrowid
    cursor.execute(
        """
        INSERT INTO documents_fts (rowid, title, body)
        VALUES (?, ?, ?)
        """,
        (doc_id, title, body),
    )


def _insert_sources(
    conn: sqlite3.Connection, sources: Dict[str, Tuple[int, List[str]]]
) -> None:
    for source_file, (row_count, columns) in sources.items():
        conn.execute(
            """
            INSERT INTO sources (source_file, row_count, columns_json)
            VALUES (?, ?, ?)
            """,
            (source_file, row_count, json.dumps(columns, ensure_ascii=True)),
        )


def _export_signature(export_dir: Path) -> str:
    hasher = hashlib.sha256()
    for path in iter_export_files(export_dir):
        stat = path.stat()
        rel_path = path.relative_to(export_dir).as_posix()
        hasher.update(rel_path.encode("utf-8"))
        hasher.update(str(stat.st_size).encode("utf-8"))
        hasher.update(str(int(stat.st_mtime)).encode("utf-8"))
    return hasher.hexdigest()


def _set_manifest(
    conn: sqlite3.Connection, signature: str, total_docs: int, export_dir: Path
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO manifest (key, value) VALUES (?, ?)",
        ("schema_version", SCHEMA_VERSION),
    )
    conn.execute(
        "INSERT OR REPLACE INTO manifest (key, value) VALUES (?, ?)",
        ("export_signature", signature),
    )
    conn.execute(
        "INSERT OR REPLACE INTO manifest (key, value) VALUES (?, ?)",
        ("total_docs", str(total_docs)),
    )
    conn.execute(
        "INSERT OR REPLACE INTO manifest (key, value) VALUES (?, ?)",
        ("export_dir", str(export_dir)),
    )


def _manifest_matches(export_dir: Path, db_path: Path) -> bool:
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error:
        return False

    try:
        cursor = conn.execute(
            "SELECT value FROM manifest WHERE key = ?", ("schema_version",)
        )
        row = cursor.fetchone()
        if not row or row[0] != SCHEMA_VERSION:
            return False

        cursor = conn.execute(
            "SELECT value FROM manifest WHERE key = ?", ("export_signature",)
        )
        row = cursor.fetchone()
        if not row:
            return False
        return row[0] == _export_signature(export_dir)
    except sqlite3.Error:
        return False
    finally:
        conn.close()
