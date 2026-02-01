from __future__ import annotations

import secrets
from pathlib import Path

from fastapi import Body, FastAPI, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from .data_loader import DATA_DIR, ensure_db
from .query import (
    answer_question,
    get_connections_by_month,
    get_recent_connection_counts,
    get_top_companies,
    get_top_industries,
    get_top_titles,
)

app = FastAPI(title="LinkedIn Q&A")
SESSION_COOKIE = "linkedin_qa_session"


def _session_dir(session_id: str) -> Path:
    return DATA_DIR / "sessions" / session_id


def _get_or_create_session_id(request: Request) -> tuple[str, bool]:
    existing = request.cookies.get(SESSION_COOKIE)
    if existing:
        return existing, False
    return secrets.token_urlsafe(16), True


def _session_paths(session_id: str) -> tuple[Path, Path]:
    session_root = _session_dir(session_id)
    export_dir = _load_session_export_dir(session_root)
    db_path = session_root / "linkedin.sqlite"
    return export_dir, db_path


def _load_session_export_dir(session_root: Path) -> Path:
    export_hint = session_root / "export_path.txt"
    if export_hint.exists():
        try:
            return Path(export_hint.read_text().strip())
        except OSError:
            return session_root / "export"
    return session_root / "export"


@app.on_event("startup")
def _startup() -> None:
    try:
        ensure_db()
    except FileNotFoundError:
        # No export uploaded yet in hosted mode.
        pass


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    session_id, created = _get_or_create_session_id(request)
    response = HTMLResponse(HTML_PAGE)
    if created:
        response.set_cookie(SESSION_COOKIE, session_id, httponly=True, samesite="lax")
    return response


@app.post("/api/ask")
def ask(request: Request, payload: dict = Body(...)) -> JSONResponse:
    question = str(payload.get("question", "")).strip()
    if not question:
        return JSONResponse(
            {"answer": "Please enter a question.", "matches": []}, status_code=400
        )
    session_id, created = _get_or_create_session_id(request)
    export_dir, db_path = _session_paths(session_id)
    try:
        ensure_db(export_dir=export_dir, db_path=db_path)
    except FileNotFoundError:
        return JSONResponse(
            {
                "answer": "No LinkedIn export uploaded yet. Upload a .zip to get started.",
                "matches": [],
            },
            status_code=400,
        )
    result = answer_question(question, db_path=db_path)
    response = JSONResponse(result)
    if created:
        response.set_cookie(SESSION_COOKIE, session_id, httponly=True, samesite="lax")
    return response


@app.post("/api/rebuild")
def rebuild(request: Request) -> JSONResponse:
    session_id, created = _get_or_create_session_id(request)
    export_dir, db_path = _session_paths(session_id)
    try:
        ensure_db(rebuild=True, export_dir=export_dir, db_path=db_path)
    except FileNotFoundError:
        return JSONResponse(
            {"error": "No LinkedIn export uploaded yet."}, status_code=400
        )
    response = JSONResponse({"status": "ok"})
    if created:
        response.set_cookie(SESSION_COOKIE, session_id, httponly=True, samesite="lax")
    return response


@app.post("/api/upload")
async def upload_export(request: Request, file: UploadFile = File(...)) -> JSONResponse:
    if not file.filename or not file.filename.lower().endswith(".zip"):
        return JSONResponse(
            {"error": "Please upload a LinkedIn export .zip file."}, status_code=400
        )

    session_id, created = _get_or_create_session_id(request)
    session_root = _session_dir(session_id)
    uploads_dir = session_root / "uploads"
    extract_dir = session_root / "export"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    zip_path = uploads_dir / "linkedin_export.zip"
    content = await file.read()
    zip_path.write_bytes(content)

    if extract_dir.exists():
        import shutil

        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    import zipfile

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    export_dir = _find_export_root(extract_dir)
    export_hint = session_root / "export_path.txt"
    export_hint.write_text(str(export_dir))
    _, db_path = _session_paths(session_id)
    ensure_db(rebuild=True, export_dir=export_dir, db_path=db_path)

    response = JSONResponse({"status": "ok"})
    if created:
        response.set_cookie(SESSION_COOKIE, session_id, httponly=True, samesite="lax")
    return response


@app.get("/api/stats")
def stats(request: Request) -> JSONResponse:
    session_id, created = _get_or_create_session_id(request)
    export_dir, db_path = _session_paths(session_id)
    try:
        ensure_db(export_dir=export_dir, db_path=db_path)
        top_companies = get_top_companies(limit=8, db_path=db_path)
        top_titles = get_top_titles(limit=8, db_path=db_path)
        top_industries = get_top_industries(limit=8, db_path=db_path)
        connections_by_month = get_connections_by_month(limit=12, db_path=db_path)
        recent_counts = get_recent_connection_counts(db_path=db_path)
    except FileNotFoundError:
        top_companies = []
        top_titles = []
        top_industries = []
        connections_by_month = []
        recent_counts = {}
    response = JSONResponse(
        {
            "top_companies": top_companies,
            "top_titles": top_titles,
            "top_industries": top_industries,
            "connections_by_month": connections_by_month,
            "recent_counts": recent_counts,
        }
    )
    if created:
        response.set_cookie(SESSION_COOKIE, session_id, httponly=True, samesite="lax")
    return response


def _find_export_root(extract_dir: Path) -> Path:
    if (extract_dir / "Connections.csv").exists():
        return extract_dir
    children = [child for child in extract_dir.iterdir() if child.is_dir()]
    if len(children) == 1:
        return children[0]
    return extract_dir


HTML_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>LinkedIn Q&A</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f4ff;
      --card: #ffffff;
      --text: #1f2933;
      --muted: #52606d;
      --border: #e4e7eb;
      --primary: #6d28d9;
      --primary-dark: #5b21b6;
      --accent: #f4e8ff;
      --accent-2: #e0f2fe;
      --accent-3: #fde68a;
      --accent-4: #dcfce7;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Inter", "Segoe UI", Helvetica, Arial, sans-serif;
      color: var(--text);
      background: var(--bg);
    }
    header {
      padding: 2.7rem 0 1.6rem;
      background: linear-gradient(120deg, #ede9fe 0%, #dbeafe 55%, #fdf2f8 100%);
      border-bottom: 1px solid var(--border);
    }
    .tabs {
      display: inline-flex;
      gap: 0.5rem;
      margin-top: 1.2rem;
      background: #ffffff;
      padding: 0.35rem;
      border-radius: 999px;
      border: 1px solid var(--border);
      box-shadow: 0 10px 25px rgba(15, 23, 42, 0.06);
    }
    .tab {
      border: none;
      background: transparent;
      padding: 0.45rem 1rem;
      border-radius: 999px;
      font-weight: 600;
      color: var(--muted);
      cursor: pointer;
      transition: all 0.2s ease;
    }
    .tab.active {
      background: linear-gradient(135deg, #7c3aed, #3b82f6);
      color: #ffffff;
      box-shadow: 0 6px 14px rgba(59, 130, 246, 0.25);
    }
    .container {
      max-width: 980px;
      margin: 0 auto;
      padding: 0 1.5rem;
    }
    h1 {
      margin: 0 0 0.5rem;
      font-size: 2.4rem;
      letter-spacing: -0.02em;
    }
    p { color: var(--muted); margin: 0; }
    .panel {
      background: var(--card);
      border-radius: 12px;
      padding: 1.5rem;
      margin-top: -1.5rem;
      box-shadow: 0 14px 40px rgba(15, 23, 42, 0.08);
      border: 1px solid var(--border);
    }
    .section {
      margin-top: 2rem;
    }
    .tab-content {
      display: none;
    }
    .tab-content.active {
      display: block;
    }
    .section-title {
      font-size: 1.2rem;
      font-weight: 600;
      margin-bottom: 0.75rem;
    }
    .label {
      font-size: 0.85rem;
      color: var(--muted);
      margin-bottom: 0.35rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    textarea {
      width: 100%;
      min-height: 120px;
      padding: 0.85rem 1rem;
      font-size: 1rem;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: #fbfbfd;
      outline: none;
      transition: border 0.2s ease, box-shadow 0.2s ease;
      resize: vertical;
    }
    textarea:focus {
      border-color: var(--primary);
      box-shadow: 0 0 0 3px rgba(29, 78, 216, 0.15);
    }
    .actions {
      display: flex;
      gap: 0.75rem;
      flex-wrap: wrap;
      margin-top: 1rem;
    }
    button {
      padding: 0.65rem 1.3rem;
      font-size: 0.95rem;
      border-radius: 8px;
      border: 1px solid transparent;
      cursor: pointer;
      transition: all 0.2s ease;
    }
    .btn-primary {
      background: linear-gradient(135deg, #7c3aed, #3b82f6);
      color: #fff;
      font-weight: 600;
    }
    .btn-primary:hover { filter: brightness(0.95); }
    .btn-secondary {
      background: #fff;
      border: 1px solid var(--border);
      color: var(--text);
    }
    .btn-secondary:hover { border-color: #cbd2d9; }
    .status {
      margin-top: 0.75rem;
      font-size: 0.95rem;
      color: var(--muted);
    }
    .answer {
      margin-top: 1.5rem;
      padding: 1rem 1.25rem;
      background: var(--accent);
      border-radius: 10px;
      border: 1px solid #d9e2ff;
      font-weight: 600;
    }
    .results-header {
      margin-top: 1.5rem;
      font-weight: 600;
      font-size: 1.05rem;
    }
    .matches {
      display: grid;
      gap: 0.85rem;
      margin-top: 0.75rem;
    }
    .match {
      padding: 1rem 1.1rem;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #fff;
    }
    .match h3 {
      margin: 0 0 0.3rem 0;
      font-size: 1rem;
    }
    .meta {
      color: var(--muted);
      font-size: 0.85rem;
    }
    .match p {
      margin: 0.5rem 0 0;
      white-space: pre-line;
      color: #374151;
    }
    .helper {
      margin-top: 0.9rem;
      font-size: 0.9rem;
      color: var(--muted);
    }
    .chips {
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
      margin-top: 0.6rem;
    }
    .chip {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      color: #334155;
      padding: 0.35rem 0.6rem;
      border-radius: 999px;
      font-size: 0.85rem;
    }
    .insights-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 1rem;
    }
    .insight-card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 1rem 1.2rem;
      box-shadow: 0 10px 25px rgba(15, 23, 42, 0.06);
    }
    .insight-card h3 {
      margin: 0 0 0.75rem 0;
      font-size: 1rem;
    }
    .upload-panel {
      background: #ffffff;
      border: 1px dashed #c7d2fe;
      border-radius: 12px;
      padding: 1rem;
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      align-items: center;
      justify-content: space-between;
    }
    .upload-panel input[type="file"] {
      font-size: 0.9rem;
    }
    .upload-status {
      font-size: 0.9rem;
      color: var(--muted);
    }
    .banner {
      margin-top: 1rem;
      padding: 0.85rem 1rem;
      border-radius: 10px;
      background: #fff7ed;
      border: 1px solid #fdba74;
      color: #9a3412;
      font-size: 0.95rem;
    }
    .stat-grid {
      display: grid;
      gap: 0.6rem;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
    }
    .stat {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 0.75rem;
    }
    .stat strong {
      display: block;
      font-size: 1.2rem;
      color: #111827;
    }
    .stat span {
      font-size: 0.85rem;
      color: var(--muted);
    }
    .bar-row {
      display: flex;
      align-items: center;
      gap: 0.6rem;
      font-size: 0.9rem;
      color: var(--muted);
    }
    .bar {
      flex: 1;
      height: 8px;
      background: #eef2ff;
      border-radius: 999px;
      position: relative;
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      border-radius: 999px;
    }
    .legend {
      margin-top: 0.75rem;
      display: grid;
      gap: 0.4rem;
      font-size: 0.9rem;
      color: var(--muted);
    }
    .legend-item {
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .logo {
      width: 24px;
      height: 24px;
      border-radius: 6px;
      background: #f1f5f9;
      border: 1px solid #e2e8f0;
      display: flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      flex-shrink: 0;
    }
    .logo img {
      width: 100%;
      height: 100%;
      object-fit: contain;
    }
    .logo-fallback {
      font-size: 0.7rem;
      color: #64748b;
      font-weight: 600;
      display: none;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
    }
    .empty-state {
      color: var(--muted);
      font-size: 0.95rem;
    }
  </style>
</head>
<body>
  <header>
    <div class="container">
      <h1>LinkedIn Q&A</h1>
      <p>Ask questions against your LinkedIn export for connections, roles, learning history, and articles.</p>
      <div class="tabs" role="tablist">
        <button class="tab active" id="tabInsights" type="button">Summary Analytics</button>
        <button class="tab" id="tabQna" type="button">Q&amp;A</button>
      </div>
    </div>
  </header>

  <main class="container">
    <section class="panel section tab-content active" id="contentInsights">
      <div class="section-title">Summary Analytics</div>
      <div id="uploadBanner" class="banner" style="display:none;">
        Upload your LinkedIn export zip to enable analytics and Q&amp;A.
      </div>
      <div class="upload-panel">
        <div>
          <strong>Update data</strong>
          <div class="upload-status" id="uploadStatus">Upload a LinkedIn export zip to refresh analytics.</div>
        </div>
        <div>
          <input type="file" id="exportFile" accept=".zip" />
          <button id="uploadBtn" class="btn-secondary">Upload</button>
        </div>
      </div>
      <div class="insights-grid">
        <div class="insight-card">
          <h3>Connections by Company</h3>
          <div id="companyChart" class="empty-state">Loading chart...</div>
          <div id="companyLegend" class="legend"></div>
        </div>
        <div class="insight-card">
          <h3>Top Job Titles</h3>
          <div id="titleList" class="legend"></div>
        </div>
        <div class="insight-card">
          <h3>Top Industries</h3>
          <div id="industryList" class="legend"></div>
        </div>
        <div class="insight-card">
          <h3>Connections Over Time</h3>
          <div id="connectionTrend" class="legend"></div>
        </div>
        <div class="insight-card">
          <h3>Recent Connections</h3>
          <div id="recentStats" class="stat-grid"></div>
        </div>
      </div>
    </section>

    <section class="panel section tab-content" id="contentQna">
      <div class="section-title">Q&amp;A</div>
      <div class="label">Your question</div>
      <textarea id="question" placeholder="Example: How many connections work at Microsoft?"></textarea>
      <div class="actions">
        <button id="askBtn" class="btn-primary">Ask</button>
        <button id="rebuildBtn" class="btn-secondary">Rebuild Index</button>
      </div>
      <div class="helper">Try these:</div>
      <div class="chips">
        <span class="chip">How many connections do I have?</span>
        <span class="chip">List connections who work at Microsoft</span>
        <span class="chip">How many articles have I written so far?</span>
        <span class="chip">Summarize themes from my articles</span>
      </div>
      <div class="status" id="status"></div>
    </section>

    <section class="section tab-content" id="contentResults">
      <div id="answer" class="answer" style="display:none;"></div>
      <div id="resultsHeader" class="results-header" style="display:none;">Results</div>
      <div id="matches" class="matches"></div>
    </section>
  </main>

  <script>
    const askBtn = document.getElementById("askBtn");
    const rebuildBtn = document.getElementById("rebuildBtn");
    const statusEl = document.getElementById("status");
    const answerEl = document.getElementById("answer");
    const resultsHeader = document.getElementById("resultsHeader");
    const matchesEl = document.getElementById("matches");
    const questionEl = document.getElementById("question");
    const companyChartEl = document.getElementById("companyChart");
    const companyLegendEl = document.getElementById("companyLegend");
    const titleListEl = document.getElementById("titleList");
    const industryListEl = document.getElementById("industryList");
    const connectionTrendEl = document.getElementById("connectionTrend");
    const recentStatsEl = document.getElementById("recentStats");
    const tabInsights = document.getElementById("tabInsights");
    const tabQna = document.getElementById("tabQna");
    const contentInsights = document.getElementById("contentInsights");
    const contentQna = document.getElementById("contentQna");
    const contentResults = document.getElementById("contentResults");
    const uploadBtn = document.getElementById("uploadBtn");
    const exportFileEl = document.getElementById("exportFile");
    const uploadStatusEl = document.getElementById("uploadStatus");
    const uploadBannerEl = document.getElementById("uploadBanner");

    const palette = ["#7c3aed", "#38bdf8", "#f59e0b", "#22c55e", "#f472b6", "#6366f1", "#14b8a6", "#fb7185"];

    document.querySelectorAll(".chip").forEach((chip) => {
      chip.addEventListener("click", () => {
        questionEl.value = chip.textContent;
        questionEl.focus();
      });
    });

    function renderPieChart(data) {
      if (!data.length) {
        companyChartEl.textContent = "No company data available.";
        return;
      }
      const total = data.reduce((acc, item) => acc + item.count, 0);
      let cumulative = 0;
      const radius = 70;
      const cx = 85;
      const cy = 85;
      const paths = data.map((item, idx) => {
        const startAngle = (cumulative / total) * Math.PI * 2;
        cumulative += item.count;
        const endAngle = (cumulative / total) * Math.PI * 2;
        const x1 = cx + radius * Math.cos(startAngle);
        const y1 = cy + radius * Math.sin(startAngle);
        const x2 = cx + radius * Math.cos(endAngle);
        const y2 = cy + radius * Math.sin(endAngle);
        const largeArc = endAngle - startAngle > Math.PI ? 1 : 0;
        return `
          <path d="M ${cx} ${cy} L ${x1} ${y1} A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2} Z"
            fill="${palette[idx % palette.length]}" />
        `;
      }).join("");
      companyChartEl.innerHTML = `
        <svg width="170" height="170" viewBox="0 0 170 170">
          ${paths}
        </svg>
      `;
    }

    function renderLegend(data) {
      companyLegendEl.innerHTML = "";
      data.forEach((item, idx) => {
        const color = palette[idx % palette.length];
        const legendItem = document.createElement("div");
        legendItem.className = "legend-item";
        const dot = document.createElement("span");
        dot.className = "dot";
        dot.style.background = color;
        const logo = createLogo(item.company);
        const text = document.createElement("span");
        text.textContent = `${item.company} • ${item.count}`;
        legendItem.appendChild(dot);
        legendItem.appendChild(logo);
        legendItem.appendChild(text);
        companyLegendEl.appendChild(legendItem);
      });
    }

    function initials(company) {
      if (!company) return "?";
      const words = company.replace(/\(.*?\)/g, "").trim().split(/\s+/).filter(Boolean);
      const letters = words.slice(0, 2).map((word) => word[0].toUpperCase());
      return letters.join("") || "?";
    }

    const logoCache = new Map();

    async function resolveLogoDomain(company) {
      if (!company) return null;
      const key = company.toLowerCase();
      if (logoCache.has(key)) {
        return logoCache.get(key);
      }
      try {
        const response = await fetch(
          `https://autocomplete.clearbit.com/v1/companies/suggest?query=${encodeURIComponent(company)}`
        );
        const data = await response.json();
        const domain = Array.isArray(data) && data.length ? data[0].domain : null;
        logoCache.set(key, domain);
        return domain;
      } catch (error) {
        logoCache.set(key, null);
        return null;
      }
    }

    function createLogo(company) {
      const wrapper = document.createElement("span");
      wrapper.className = "logo";
      const img = document.createElement("img");
      const fallback = document.createElement("span");
      fallback.className = "logo-fallback";
      fallback.textContent = initials(company);
      img.alt = company;
      img.onerror = () => {
        img.style.display = "none";
        fallback.style.display = "flex";
      };
      img.onload = () => {
        fallback.style.display = "none";
      };
      wrapper.appendChild(img);
      wrapper.appendChild(fallback);
      resolveLogoDomain(company).then((domain) => {
        if (!domain) {
          img.style.display = "none";
          fallback.style.display = "flex";
          return;
        }
        img.src = `https://logo.clearbit.com/${domain}`;
      });
      return wrapper;
    }

    function renderBarList(target, items, keyName) {
      target.innerHTML = "";
      if (!items.length) {
        target.textContent = "No data available.";
        return;
      }
      const max = Math.max(...items.map((item) => item.count));
      items.forEach((item, idx) => {
        const row = document.createElement("div");
        row.className = "bar-row";
        const label = document.createElement("div");
        label.style.minWidth = "130px";
        label.textContent = item[keyName];
        const bar = document.createElement("div");
        bar.className = "bar";
        const fill = document.createElement("div");
        fill.className = "bar-fill";
        fill.style.width = `${Math.max(8, (item.count / max) * 100)}%`;
        fill.style.background = palette[idx % palette.length];
        bar.appendChild(fill);
        const count = document.createElement("div");
        count.textContent = item.count;
        row.appendChild(label);
        row.appendChild(bar);
        row.appendChild(count);
        target.appendChild(row);
      });
    }

    function renderRecentStats(counts) {
      recentStatsEl.innerHTML = "";
      const items = [
        { label: "Last 30 days", value: counts["30d"] || 0 },
        { label: "Last 90 days", value: counts["90d"] || 0 },
        { label: "Last 12 months", value: counts["365d"] || 0 },
      ];
      items.forEach((item) => {
        const card = document.createElement("div");
        card.className = "stat";
        card.innerHTML = `<strong>${item.value}</strong><span>${item.label}</span>`;
        recentStatsEl.appendChild(card);
      });
    }

    async function loadStats() {
      try {
        const response = await fetch("/api/stats");
        const data = await response.json();
        const topCompanies = data.top_companies || [];
        const connectionsByMonth = data.connections_by_month || [];
        const recentCounts = data.recent_counts || {};
        const hasData =
          topCompanies.length ||
          connectionsByMonth.length ||
          (recentCounts["30d"] || 0) +
            (recentCounts["90d"] || 0) +
            (recentCounts["365d"] || 0) >
            0;
        uploadBannerEl.style.display = hasData ? "none" : "block";
        renderPieChart(topCompanies);
        renderLegend(topCompanies);
        renderBarList(titleListEl, data.top_titles || [], "title");
        renderBarList(industryListEl, data.top_industries || [], "industry");
        renderBarList(connectionTrendEl, connectionsByMonth, "month");
        renderRecentStats(recentCounts);
      } catch (error) {
        companyChartEl.textContent = "Failed to load chart data.";
        uploadBannerEl.style.display = "block";
      }
    }

    function clearResults() {
      answerEl.style.display = "none";
      answerEl.textContent = "";
      resultsHeader.style.display = "none";
      matchesEl.innerHTML = "";
    }

    function setActiveTab(tab) {
      if (tab === "insights") {
        tabInsights.classList.add("active");
        tabQna.classList.remove("active");
        contentInsights.classList.add("active");
        contentQna.classList.remove("active");
        contentResults.classList.remove("active");
      } else {
        tabInsights.classList.remove("active");
        tabQna.classList.add("active");
        contentInsights.classList.remove("active");
        contentQna.classList.add("active");
        contentResults.classList.add("active");
      }
    }

    tabInsights.addEventListener("click", () => setActiveTab("insights"));
    tabQna.addEventListener("click", () => setActiveTab("qna"));

    uploadBtn.addEventListener("click", async () => {
      const file = exportFileEl.files[0];
      if (!file) {
        uploadStatusEl.textContent = "Please select a .zip export to upload.";
        return;
      }
      uploadStatusEl.textContent = "Uploading and reindexing...";
      const formData = new FormData();
      formData.append("file", file);
      const response = await fetch("/api/upload", { method: "POST", body: formData });
      const data = await response.json();
      if (response.ok) {
        uploadStatusEl.textContent = "Upload complete. Analytics updated.";
        loadStats();
      } else {
        uploadStatusEl.textContent = data.error || "Upload failed.";
      }
    });

    askBtn.addEventListener("click", async () => {
      const question = questionEl.value.trim();
      if (!question) {
        statusEl.textContent = "Please enter a question.";
        return;
      }
      statusEl.textContent = "Searching your export...";
      clearResults();

      const response = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question })
      });

      const data = await response.json();
      statusEl.textContent = "";

      if (data.answer) {
        answerEl.textContent = data.answer;
        answerEl.style.display = "block";
        setActiveTab("qna");
      }

      if (Array.isArray(data.matches) && data.matches.length > 0) {
        resultsHeader.style.display = "block";
        data.matches.forEach((match) => {
          const card = document.createElement("div");
          card.className = "match";

          const title = document.createElement("h3");
          title.textContent = match.title || "Match";
          card.appendChild(title);

          const meta = document.createElement("div");
          meta.className = "meta";
          meta.textContent = `${match.source_file} • row ${match.row_id}`;
          card.appendChild(meta);

          if (match.snippet) {
            const snippet = document.createElement("p");
            snippet.textContent = match.snippet;
            card.appendChild(snippet);
          }

          matchesEl.appendChild(card);
        });
        setActiveTab("qna");
      }
    });

    rebuildBtn.addEventListener("click", async () => {
      statusEl.textContent = "Rebuilding index...";
      clearResults();
      await fetch("/api/rebuild", { method: "POST" });
      statusEl.textContent = "Index rebuilt.";
      loadStats();
    });

    setActiveTab("insights");
    loadStats();
  </script>
</body>
</html>
"""
