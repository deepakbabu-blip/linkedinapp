from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from .data_loader import DB_PATH


SOURCE_ALIASES: Dict[str, str] = {
    "connections": "Connections.csv",
    "connection": "Connections.csv",
    "positions": "Positions.csv",
    "position": "Positions.csv",
    "profile": "Profile.csv",
    "profile summary": "Profile Summary.csv",
    "recommendations": "Recommendations_Given.csv",
    "recommendation": "Recommendations_Given.csv",
    "learning": "Learning.csv",
    "courses": "Learning.csv",
    "events": "Events.csv",
    "company follows": "Company Follows.csv",
    "ad targeting": "Ad_Targeting.csv",
    "email addresses": "Email Addresses.csv",
    "phone numbers": "PhoneNumbers.csv",
    "whatsapp": "Whatsapp Phone Numbers.csv",
    "job applications": "Jobs/Job Applications.csv",
    "saved jobs": "Jobs/Saved Jobs.csv",
    "job seeker preferences": "Jobs/Job Seeker Preferences.csv",
    "screening questions": "Job Applicant Saved Screening Question Responses.csv",
    "saved answers": "Jobs/Job Applicant Saved Answers.csv",
}


def answer_question(
    question: str, limit: int = 8, db_path: Path | None = None
) -> Dict[str, object]:
    cleaned = question.strip()
    if not cleaned:
        return {"answer": "Ask a question to get started.", "matches": []}

    source = infer_source(cleaned)

    if is_article_theme_question(cleaned):
        themes = summarize_article_themes(top_n=8, db_path=db_path)
        if not themes:
            return {
                "answer": "I couldn't find any articles to summarize.",
                "matches": [],
                "source": "Articles",
            }
        answer = "Top conceptual themes across your articles."
        matches = [
            {
                "source_file": theme["source_file"],
                "row_id": theme["row_id"],
                "title": theme["title"],
                "snippet": theme["snippet"],
            }
            for theme in themes
        ]
        return {"answer": answer, "matches": matches, "source": "Articles"}

    popular_request = parse_article_popularity_question(cleaned)
    article_total = parse_article_total_question(cleaned)
    if article_total or popular_request:
        count = count_all_articles(db_path=db_path)
        answer = f"You have published {count} articles so far."
        matches: List[Dict[str, object]] = []
        if popular_request:
            popular = list_most_popular_articles(limit=5, db_path=db_path)
            if popular:
                answer += (
                    " The export does not include views/likes, so popularity is "
                    "approximated by longer articles."
                )
                matches = popular
            else:
                answer += " I couldn't estimate popularity because article lengths were missing."
        return {
            "answer": answer,
            "matches": matches,
            "source": "Articles",
        }

    article_days = parse_article_days_question(cleaned)
    if article_days is not None:
        count, matches, missing = count_articles_last_days(
            article_days, limit=10, db_path=db_path
        )
        answer = f"You published {count} articles in the last {article_days} days."
        if missing:
            answer += f" ({missing} article(s) missing a created date were skipped.)"
        return {
            "answer": answer,
            "matches": matches,
            "source": "Articles",
            "days": article_days,
        }

    recent_company = parse_recent_company_question(cleaned)
    if recent_company:
        matches = get_recent_connections_by_company(
            recent_company, limit=5, db_path=db_path
        )
        if matches:
            answer = (
                "Here are your most recently added connections at "
                f"{recent_company}."
            )
        else:
            answer = f"No recent connections found for {recent_company}."
        return {
            "answer": answer,
            "matches": matches,
            "source": "Connections.csv",
            "company": recent_company,
        }

    company_list, operator = parse_company_list_question(cleaned)
    if len(company_list) > 1:
        if is_count_question(cleaned):
            if operator == "both":
                count = count_connections_at_all_companies(
                    company_list, db_path=db_path
                )
                return {
                    "answer": (
                        "You have "
                        f"{count} connections whose current company matches all of: "
                        f"{', '.join(company_list)}. "
                        "LinkedIn exports only include current company, not full work "
                        "history for connections."
                    ),
                    "matches": [],
                    "source": "Connections.csv",
                    "companies": company_list,
                }
            counts = {
                company: count_connections_by_company(company, db_path=db_path)
                for company in company_list
            }
            summary = "; ".join(f"{company}: {count}" for company, count in counts.items())
            return {
                "answer": (
                    "Counts by company (current company only): "
                    f"{summary}."
                ),
                "matches": [],
                "source": "Connections.csv",
                "companies": company_list,
            }
        matches = get_connections_by_companies(company_list, limit=80, db_path=db_path)
        if matches:
            answer = (
                "Here are connections whose current company matches any of: "
                f"{', '.join(company_list)}."
            )
        else:
            answer = f"No connections found for {', '.join(company_list)}."
        return {
            "answer": answer,
            "matches": matches,
            "source": "Connections.csv",
            "companies": company_list,
        }

    referral_company = parse_referral_company(cleaned)
    if referral_company:
        matches = get_connections_by_company(
            referral_company, limit=50, db_path=db_path
        )
        if matches:
            answer = f"Here are connections who could refer you at {referral_company}."
        else:
            answer = f"No connections found for {referral_company}."
        return {
            "answer": answer,
            "matches": matches,
            "source": "Connections.csv",
            "company": referral_company,
        }

    company = parse_company_question(cleaned)
    if company:
        if is_count_question(cleaned):
            count = count_connections_by_company(company, db_path=db_path)
            return {
                "answer": f"You have {count} connections at {company}.",
                "matches": [],
                "source": "Connections.csv",
                "company": company,
            }
        matches = get_connections_by_company(company, limit=50, db_path=db_path)
        if matches:
            answer = f"Here are your connections who work at {company}."
        else:
            answer = f"No connections found for {company}."
        return {
            "answer": answer,
            "matches": matches,
            "source": "Connections.csv",
            "company": company,
        }

    if is_engagement_question(cleaned):
        recent = get_recent_connections(limit=10, db_path=db_path)
        answer = (
            "Your LinkedIn export does not include interaction counts, so I can't "
            "measure engagement directly. Here are your most recently added connections "
            "as a nearby proxy."
        )
        return {"answer": answer, "matches": recent, "source": "Connections.csv"}

    if is_count_question(cleaned) and source:
        count = get_source_count(source, db_path=db_path)
        if count is None:
            return {
                "answer": f"I couldn't find any rows for {source}.",
                "matches": [],
                "source": source,
            }
        return {
            "answer": f"There are {count} rows in {source}.",
            "matches": [],
            "source": source,
        }

    matches = search_documents(cleaned, source=source, limit=limit, db_path=db_path)
    if matches:
        answer = "Here are the closest matches from your LinkedIn export."
    else:
        answer = "No direct matches found. Try different keywords or a broader query."

    return {"answer": answer, "matches": matches, "source": source}


def infer_source(question: str) -> Optional[str]:
    lowered = question.lower()
    for alias in sorted(SOURCE_ALIASES.keys(), key=len, reverse=True):
        if alias in lowered:
            return SOURCE_ALIASES[alias]
    return None


def is_count_question(question: str) -> bool:
    return bool(re.search(r"\b(how many|count|number of|total)\b", question.lower()))


def is_engagement_question(question: str) -> bool:
    lowered = question.lower()
    return any(term in lowered for term in ["engage", "interaction", "interact", "talk to"]) and (
        "most" in lowered or "top" in lowered
    )


def is_article_theme_question(question: str) -> bool:
    lowered = question.lower()
    return "article" in lowered and any(
        term in lowered
        for term in ["themes", "summary", "summarize", "topics", "analyze", "concept"]
    )


def parse_company_question(question: str) -> Optional[str]:
    if "connection" not in question.lower():
        return None
    pattern = re.compile(
        r"(?:connections?\s+(?:who\s+)?(?:work(?:s|ing)?\s+(?:at|for)|employed\s+by|at))\s+(.+?)(?:\?|$)",
        re.IGNORECASE,
    )
    match = pattern.search(question)
    if not match:
        lowered = question.lower()
        if "connections" in lowered and " at " in lowered:
            company = lowered.rsplit(" at ", 1)[-1]
            company = question[len(question) - len(company) :].strip()
        else:
            return None
    else:
        company = match.group(1).strip().strip('"').strip("'")
    if not company:
        return None
    return company


def parse_referral_company(question: str) -> Optional[str]:
    lowered = question.lower()
    if "refer" not in lowered:
        return None
    pattern = re.compile(
        r"refer\s+me\s+to\s+(.+?)(?:\s+(?:jobs?|roles?|positions?))?(?:\?|$)",
        re.IGNORECASE,
    )
    match = pattern.search(question)
    if not match:
        pattern = re.compile(
            r"refer\s+me\s+.*?\s+at\s+(.+?)(?:\s+(?:jobs?|roles?|positions?))?(?:\?|$)",
            re.IGNORECASE,
        )
        match = pattern.search(question)
    if not match:
        return None
    company = match.group(1).strip().strip('"').strip("'")
    if not company:
        return None
    return company


def parse_company_list_question(question: str) -> tuple[List[str], str]:
    lowered = question.lower()
    if "connection" not in lowered or " at " not in lowered:
        return [], "any"
    pattern = re.compile(
        r"(?:connections?\s+(?:who\s+)?(?:have\s+worked\s+at|work(?:s|ing)?\s+(?:at|for)|employed\s+by|at))\s+(.+?)(?:\?|$)",
        re.IGNORECASE,
    )
    match = pattern.search(question)
    if not match:
        return [], "any"
    companies_blob = match.group(1).strip()
    if not companies_blob:
        return [], "any"
    operator = "any"
    if re.search(r"\bboth\b", companies_blob, re.IGNORECASE):
        operator = "both"
    if re.search(r"\beither\b", companies_blob, re.IGNORECASE):
        operator = "any"
    parts = re.split(r"\s+(?:and|or)\s+|,\s*", companies_blob)
    companies = []
    for part in parts:
        cleaned = part.strip().strip('"').strip("'")
        cleaned = re.sub(r"^(both|either)\s+", "", cleaned, flags=re.IGNORECASE)
        if cleaned:
            companies.append(cleaned)
    return companies, operator


def parse_article_days_question(question: str) -> Optional[int]:
    lowered = question.lower()
    if "article" not in lowered:
        return None
    match = re.search(r"last\s+(\d+)\s+days?", lowered)
    if match:
        return int(match.group(1))
    if "last month" in lowered:
        return 30
    return None


def parse_article_total_question(question: str) -> bool:
    lowered = question.lower()
    if "article" not in lowered:
        return False
    return any(
        term in lowered
        for term in ["so far", "total", "overall", "to date", "all time"]
    ) or is_count_question(lowered)


def parse_article_popularity_question(question: str) -> bool:
    lowered = question.lower()
    return "article" in lowered and any(
        term in lowered for term in ["popular", "most popular", "top", "best performing"]
    )


def parse_recent_company_question(question: str) -> Optional[str]:
    lowered = question.lower()
    if "connection" not in lowered:
        return None
    if not any(term in lowered for term in ["most recent", "recently", "latest"]):
        return None
    if " from " not in lowered and " at " not in lowered:
        return None
    pattern = re.compile(
        r"(?:connections?\s+(?:from|at))\s+(.+?)(?:\?|$)",
        re.IGNORECASE,
    )
    match = pattern.search(question)
    if not match:
        return None
    company = match.group(1).strip().strip('"').strip("'")
    if not company:
        return None
    return company


def search_documents(
    question: str,
    source: Optional[str] = None,
    limit: int = 8,
    db_path: Path | None = None,
) -> List[Dict[str, object]]:
    tokens = re.findall(r"[a-zA-Z0-9][a-zA-Z0-9\-\+]*", question.lower())
    if tokens:
        fts_query = " ".join(tokens[:15])
    else:
        fts_query = question.strip()

    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT
                d.source_file,
                d.row_id,
                d.title,
                snippet(documents_fts, 1, '[', ']', '…', 18) AS snippet
            FROM documents_fts
            JOIN documents d ON d.id = documents_fts.rowid
            WHERE documents_fts MATCH ?
        """
        params: List[object] = [fts_query]
        if source:
            sql += " AND d.source_file = ?"
            params.append(source)
        sql += " ORDER BY bm25(documents_fts) LIMIT ?"
        params.append(limit)

        rows = conn.execute(sql, params).fetchall()
        return [
            {
                "source_file": row["source_file"],
                "row_id": row["row_id"],
                "title": row["title"],
                "snippet": row["snippet"],
            }
            for row in rows
        ]
    finally:
        conn.close()


def get_connections_by_company(
    company: str, limit: int = 50, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    company_lower = company.lower()
    try:
        sql = """
            SELECT row_id, title, body, data_json
            FROM documents
            WHERE source_file = ?
              AND LOWER(body) LIKE ?
            ORDER BY row_id ASC
            LIMIT ?
        """
        pattern = f"%company: {company_lower}%"
        rows = conn.execute(sql, ("Connections.csv", pattern, limit)).fetchall()
        if not rows:
            pattern = f"%{company_lower}%"
            rows = conn.execute(sql, ("Connections.csv", pattern, limit)).fetchall()
        return [_format_connection_row(row, "Connections.csv") for row in rows]
    finally:
        conn.close()


def count_connections_by_company(company: str, db_path: Path | None = None) -> int:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    company_lower = company.lower()
    try:
        sql = """
            SELECT COUNT(*)
            FROM documents
            WHERE source_file = ?
              AND LOWER(body) LIKE ?
        """
        pattern = f"%company: {company_lower}%"
        row = conn.execute(sql, ("Connections.csv", pattern)).fetchone()
        count = int(row[0]) if row else 0
        if count == 0:
            pattern = f"%{company_lower}%"
            row = conn.execute(sql, ("Connections.csv", pattern)).fetchone()
            count = int(row[0]) if row else 0
        return count
    finally:
        conn.close()


def count_connections_at_all_companies(
    companies: List[str], db_path: Path | None = None
) -> int:
    if not companies:
        return 0
    conn = sqlite3.connect(str(db_path or DB_PATH))
    try:
        base_query = """
            SELECT row_id
            FROM documents
            WHERE source_file = ?
              AND LOWER(body) LIKE ?
        """
        row_sets = []
        for company in companies:
            company_lower = company.lower()
            pattern = f"%company: {company_lower}%"
            rows = conn.execute(
                base_query, ("Connections.csv", pattern)
            ).fetchall()
            row_ids = {row[0] for row in rows}
            row_sets.append(row_ids)
        if not row_sets:
            return 0
        intersection = set.intersection(*row_sets)
        return len(intersection)
    finally:
        conn.close()


def count_articles_last_days(
    days: int, limit: int = 10, db_path: Path | None = None
) -> tuple[int, List[Dict[str, object]], int]:
    cutoff = datetime.now() - timedelta(days=days)
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    count = 0
    matches: List[Dict[str, object]] = []
    missing_date = 0
    try:
        rows = conn.execute(
            """
            SELECT source_file, row_id, title, body
            FROM documents
            WHERE source_file LIKE ?
            """,
            ("Articles/Articles/%",),
        ).fetchall()
        for row in rows:
            created = _extract_article_created(row["body"], row["source_file"])
            if not created:
                missing_date += 1
                continue
            if created < cutoff:
                continue
            count += 1
            if len(matches) < limit:
                matches.append(
                    {
                        "source_file": row["source_file"],
                        "row_id": row["row_id"],
                        "title": row["title"],
                        "snippet": f"Created on: {created.strftime('%Y-%m-%d %H:%M')}",
                    }
                )
        return count, matches, missing_date
    finally:
        conn.close()


def count_all_articles(db_path: Path | None = None) -> int:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM documents
            WHERE source_file LIKE ?
            """,
            ("Articles/Articles/%",),
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def list_most_popular_articles(
    limit: int = 5, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    entries: List[Dict[str, object]] = []
    try:
        rows = conn.execute(
            """
            SELECT source_file, row_id, title, body
            FROM documents
            WHERE source_file LIKE ?
            """,
            ("Articles/Articles/%",),
        ).fetchall()
        for row in rows:
            body = row["body"] or ""
            word_count = len(re.findall(r"[a-zA-Z0-9]+", body))
            if word_count == 0:
                continue
            entries.append(
                {
                    "source_file": row["source_file"],
                    "row_id": row["row_id"],
                    "title": row["title"],
                    "snippet": f"Length estimate: {word_count} words",
                    "_sort_key": word_count,
                }
            )
        entries.sort(key=lambda item: item["_sort_key"], reverse=True)
        for item in entries[:limit]:
            item.pop("_sort_key", None)
        return entries[:limit]
    finally:
        conn.close()


def get_top_companies(
    limit: int = 10, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    counts: Dict[str, int] = {}
    try:
        rows = conn.execute(
            """
            SELECT data_json
            FROM documents
            WHERE source_file = ?
            """,
            ("Connections.csv",),
        ).fetchall()
        for row in rows:
            data = _safe_json_load(row["data_json"])
            company = (data.get("Company") or "").strip()
            if not company:
                continue
            counts[company] = counts.get(company, 0) + 1

        top = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]
        return [{"company": name, "count": count} for name, count in top]
    finally:
        conn.close()


def get_top_titles(limit: int = 10, db_path: Path | None = None) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    counts: Dict[str, int] = {}
    try:
        rows = conn.execute(
            """
            SELECT data_json
            FROM documents
            WHERE source_file = ?
            """,
            ("Connections.csv",),
        ).fetchall()
        for row in rows:
            data = _safe_json_load(row["data_json"])
            title = (data.get("Position") or "").strip()
            if not title:
                continue
            counts[title] = counts.get(title, 0) + 1

        top = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]
        return [{"title": name, "count": count} for name, count in top]
    finally:
        conn.close()


def get_top_industries(
    limit: int = 10, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    counts: Dict[str, int] = {}
    try:
        rows = conn.execute(
            """
            SELECT data_json
            FROM documents
            WHERE source_file = ?
            """,
            ("Connections.csv",),
        ).fetchall()
        for row in rows:
            data = _safe_json_load(row["data_json"])
            industry = (data.get("Industry") or "").strip()
            if not industry:
                continue
            counts[industry] = counts.get(industry, 0) + 1

        top = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:limit]
        return [{"industry": name, "count": count} for name, count in top]
    finally:
        conn.close()


def get_connections_by_month(
    limit: int = 12, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    counts: Dict[str, int] = {}
    try:
        rows = conn.execute(
            """
            SELECT data_json
            FROM documents
            WHERE source_file = ?
            """,
            ("Connections.csv",),
        ).fetchall()
        for row in rows:
            data = _safe_json_load(row["data_json"])
            date_str = data.get("Connected On", "")
            parsed = _parse_connection_date(date_str)
            if not parsed:
                continue
            key = parsed.strftime("%Y-%m")
            counts[key] = counts.get(key, 0) + 1

        if not counts:
            return []
        sorted_items = sorted(counts.items(), key=lambda item: item[0])
        if len(sorted_items) > limit:
            sorted_items = sorted_items[-limit:]
        return [{"month": month, "count": count} for month, count in sorted_items]
    finally:
        conn.close()


def get_recent_connection_counts(db_path: Path | None = None) -> Dict[str, int]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    now = datetime.now()
    ranges = {"30d": 30, "90d": 90, "365d": 365}
    counts = {key: 0 for key in ranges}
    try:
        rows = conn.execute(
            """
            SELECT data_json
            FROM documents
            WHERE source_file = ?
            """,
            ("Connections.csv",),
        ).fetchall()
        for row in rows:
            data = _safe_json_load(row["data_json"])
            date_str = data.get("Connected On", "")
            parsed = _parse_connection_date(date_str)
            if not parsed:
                continue
            delta_days = (now - parsed).days
            for key, days in ranges.items():
                if 0 <= delta_days <= days:
                    counts[key] += 1
        return counts
    finally:
        conn.close()


def summarize_article_themes(
    top_n: int = 8, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    concepts = _concept_themes()
    concept_counts: Dict[str, int] = {name: 0 for name in concepts}
    concept_examples: Dict[str, Dict[str, object]] = {}
    try:
        rows = conn.execute(
            """
            SELECT source_file, row_id, title, body
            FROM documents
            WHERE source_file LIKE ?
            """,
            ("Articles/Articles/%",),
        ).fetchall()
        for row in rows:
            text = f"{row['title']}\n{row['body'] or ''}".lower()
            for concept, patterns in concepts.items():
                if _text_matches_any(text, patterns):
                    concept_counts[concept] += 1
                    if concept not in concept_examples:
                        concept_examples[concept] = {
                            "source_file": row["source_file"],
                            "row_id": row["row_id"],
                            "title": row["title"],
                        }

        if not any(count > 0 for count in concept_counts.values()):
            return []

        top_concepts = sorted(
            concept_counts.items(), key=lambda item: item[1], reverse=True
        )[:top_n]
        results = []
        for concept, count in top_concepts:
            if count == 0:
                continue
            example = concept_examples.get(concept)
            if not example:
                continue
            results.append(
                {
                    "source_file": example["source_file"],
                    "row_id": example["row_id"],
                    "title": example["title"],
                    "snippet": f"Theme: {concept} (articles: {count})",
                }
            )
        return results
    finally:
        conn.close()


def _extract_article_created(body: str, source_file: str) -> Optional[datetime]:
    if body:
        match = re.search(r"Created on\s+(\d{4}-\d{2}-\d{2})(?:\s+(\d{2}:\d{2}))?", body)
        if match:
            date_part = match.group(1)
            time_part = match.group(2) or "00:00"
            try:
                return datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M")
            except ValueError:
                pass
    filename = source_file.rsplit("/", 1)[-1]
    match = re.match(r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})", filename)
    if match:
        try:
            return datetime.strptime(
                f"{match.group(1)} {match.group(2)}", "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            return None
    return None


def _extract_rake_phrases(
    text: str, stopwords: set[str]
) -> tuple[List[tuple[str, float]], Dict[str, float]]:
    cleaned = re.sub(r"[^a-zA-Z0-9\s\-]", " ", text.lower())
    tokens = cleaned.split()
    phrases: List[List[str]] = []
    current: List[str] = []
    for token in tokens:
        if token in stopwords or len(token) <= 2:
            if current:
                phrases.append(current)
                current = []
        else:
            current.append(token)
    if current:
        phrases.append(current)

    word_freq: Dict[str, int] = {}
    word_degree: Dict[str, int] = {}
    for phrase in phrases:
        unique_tokens = [tok for tok in phrase if tok]
        length = len(unique_tokens)
        for tok in unique_tokens:
            word_freq[tok] = word_freq.get(tok, 0) + 1
            word_degree[tok] = word_degree.get(tok, 0) + (length - 1)

    word_scores: Dict[str, float] = {}
    for tok, freq in word_freq.items():
        word_scores[tok] = (word_degree.get(tok, 0) + freq) / max(freq, 1)

    phrase_scores: Dict[str, float] = {}
    for phrase in phrases:
        if not phrase:
            continue
        if len(phrase) > 5:
            continue
        phrase_text = " ".join(phrase)
        score = sum(word_scores.get(tok, 0.0) for tok in phrase)
        phrase_scores[phrase_text] = max(phrase_scores.get(phrase_text, 0.0), score)

    ranked = sorted(phrase_scores.items(), key=lambda item: item[1], reverse=True)
    return ranked, word_scores


def _text_matches_any(text: str, patterns: List[str]) -> bool:
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def _concept_themes() -> Dict[str, List[str]]:
    return {
        "AI agents & autonomy": [
            r"\bagentic\b",
            r"\bagents?\b",
            r"\bautonom(?:ous|y)\b",
            r"\borchestration\b",
            r"\bworkflow\b",
        ],
        "Evaluation & reliability": [
            r"\bevals?\b",
            r"\bevaluation\b",
            r"\bbenchmark\b",
            r"\breliab(?:ility|le)\b",
            r"\btesting\b",
        ],
        "Observability & monitoring": [
            r"\bobservability\b",
            r"\bmonitoring\b",
            r"\btelemetry\b",
            r"\btrace\b",
        ],
        "Retrieval, RAG & context": [
            r"\brag\b",
            r"\bretrieval\b",
            r"\bcontext\b",
            r"\bknowledge\b",
        ],
        "Security, safety & guardrails": [
            r"\bsecurity\b",
            r"\bsafety\b",
            r"\bguardrails?\b",
            r"\bcompliance\b",
            r"\brisk\b",
        ],
        "Product strategy & GTM": [
            r"\bproduct\b",
            r"\bpricing\b",
            r"\bgo[- ]to[- ]market\b",
            r"\bpositioning\b",
            r"\bstrategy\b",
        ],
        "Leadership & org design": [
            r"\bleadership\b",
            r"\borganization\b",
            r"\bteam\b",
            r"\bmanagement\b",
            r"\bculture\b",
        ],
        "Careers & skills": [
            r"\bcareer\b",
            r"\bjobs?\b",
            r"\bskills?\b",
            r"\bhiring\b",
            r"\btalent\b",
        ],
        "Infrastructure & platforms": [
            r"\bplatform\b",
            r"\bhosting\b",
            r"\binfrastructure\b",
            r"\bdeployment\b",
            r"\bcloud\b",
        ],
    }


def _stopwords() -> set[str]:
    return {
        "the",
        "and",
        "for",
        "with",
        "your",
        "you",
        "are",
        "this",
        "that",
        "from",
        "have",
        "has",
        "how",
        "what",
        "why",
        "into",
        "over",
        "more",
        "most",
        "than",
        "their",
        "about",
        "will",
        "can",
        "our",
        "its",
        "not",
        "but",
        "all",
        "use",
        "using",
        "yourself",
        "across",
        "between",
        "guide",
        "after",
        "before",
        "they",
        "them",
        "out",
        "when",
        "who",
        "been",
        "was",
        "were",
        "also",
        "new",
        "first",
        "last",
        "next",
        "each",
        "within",
        "without",
        "under",
        "every",
        "many",
        "much",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "today",
        "tomorrow",
        "years",
        "year",
        "day",
        "days",
        "week",
        "weeks",
        "month",
        "months",
        "article",
        "articles",
        "linkedin",
        "just",
        "like",
        "make",
        "made",
        "making",
        "get",
        "getting",
        "help",
        "helps",
        "helping",
        "think",
        "thinking",
        "need",
        "needs",
        "needed",
        "ai",
        "genai",
        "agent",
        "agents",
    }


def get_connections_by_companies(
    companies: List[str], limit: int = 80, db_path: Path | None = None
) -> List[Dict[str, object]]:
    seen = set()
    matches: List[Dict[str, object]] = []
    per_company_limit = max(5, limit // max(1, len(companies)))
    for company in companies:
        rows = get_connections_by_company(
            company, limit=per_company_limit, db_path=db_path
        )
        for row in rows:
            key = (row["source_file"], row["row_id"])
            if key in seen:
                continue
            seen.add(key)
            row["snippet"] = f"Company match: {company}\n{row.get('snippet', '')}".strip()
            matches.append(row)
            if len(matches) >= limit:
                return matches
    return matches


def get_recent_connections(
    limit: int = 10, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    entries: List[Dict[str, object]] = []
    try:
        rows = conn.execute(
            """
            SELECT row_id, title, data_json
            FROM documents
            WHERE source_file = ?
            """,
            ("Connections.csv",),
        ).fetchall()
        for row in rows:
            data = _safe_json_load(row["data_json"])
            date_str = data.get("Connected On", "")
            parsed = _parse_connection_date(date_str)
            if not parsed:
                continue
            entry = _format_connection_from_data(row, data, "Connections.csv")
            entry["connected_on"] = date_str
            entry["_sort_key"] = parsed
            entries.append(entry)

        entries.sort(key=lambda item: item["_sort_key"], reverse=True)
        for item in entries[:limit]:
            item.pop("_sort_key", None)
        return entries[:limit]
    finally:
        conn.close()


def get_recent_connections_by_company(
    company: str, limit: int = 5, db_path: Path | None = None
) -> List[Dict[str, object]]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    conn.row_factory = sqlite3.Row
    entries: List[Dict[str, object]] = []
    company_lower = company.lower()
    try:
        rows = conn.execute(
            """
            SELECT row_id, title, data_json
            FROM documents
            WHERE source_file = ?
            """,
            ("Connections.csv",),
        ).fetchall()
        for row in rows:
            data = _safe_json_load(row["data_json"])
            company_value = data.get("Company", "")
            if not company_value:
                continue
            if company_value.lower() != company_lower:
                continue
            date_str = data.get("Connected On", "")
            parsed = _parse_connection_date(date_str)
            if not parsed:
                continue
            entry = _format_connection_from_data(row, data, "Connections.csv")
            entry["connected_on"] = date_str
            entry["_sort_key"] = parsed
            entries.append(entry)

        entries.sort(key=lambda item: item["_sort_key"], reverse=True)
        for item in entries[:limit]:
            item.pop("_sort_key", None)
        return entries[:limit]
    finally:
        conn.close()


def _format_connection_row(row: sqlite3.Row, source: str) -> Dict[str, object]:
    data = _safe_json_load(row["data_json"])
    return _format_connection_from_data(row, data, source)


def _format_connection_from_data(
    row: sqlite3.Row, data: Dict[str, str], source: str
) -> Dict[str, object]:
    first = data.get("First Name", "").strip()
    last = data.get("Last Name", "").strip()
    name = " ".join(part for part in [first, last] if part).strip()
    title = name or row["title"] or "Connection"
    company = data.get("Company", "")
    position = data.get("Position", "")
    connected_on = data.get("Connected On", "")
    url = data.get("URL", "")

    snippet_parts = []
    if position:
        snippet_parts.append(f"Position: {position}")
    if company:
        snippet_parts.append(f"Company: {company}")
    if connected_on:
        snippet_parts.append(f"Connected On: {connected_on}")
    if url:
        snippet_parts.append(f"URL: {url}")

    return {
        "source_file": source,
        "row_id": row["row_id"],
        "title": title,
        "snippet": "\n".join(snippet_parts),
    }


def _safe_json_load(data_json: str) -> Dict[str, str]:
    try:
        parsed = json.loads(data_json)
        if isinstance(parsed, dict):
            return {str(k): "" if v is None else str(v) for k, v in parsed.items()}
    except json.JSONDecodeError:
        return {}
    return {}


def _parse_connection_date(value: str) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def get_source_count(
    source_file: str, db_path: Path | None = None
) -> Optional[int]:
    conn = sqlite3.connect(str(db_path or DB_PATH))
    try:
        row = conn.execute(
            "SELECT row_count FROM sources WHERE source_file = ?",
            (source_file,),
        ).fetchone()
        if not row:
            return None
        return int(row[0])
    finally:
        conn.close()
