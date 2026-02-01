## LinkedIn Q&A app

Local FastAPI app to ask questions against your LinkedIn export.

### Setup

1) Ensure the export folder is available at:
`src/agent/Basic_LinkedInDataExport_01-31-2026.zip`

2) Install dependencies (already in `requirements.txt`):
`pip install -r requirements.txt`

3) Start the app:
`PYTHONPATH=src uvicorn agent.linkedin_qa_app.app:app --reload`

### Sharing with friends (hosted)

- Deploy with Render using `render.yaml` in the repo root.
- The app now supports per-user uploads. Each browser session gets its own
  isolated dataset and database.
- Friends can open the URL and upload their LinkedIn export zip from the
  Summary Analytics tab.

### Configuration

- `LINKEDIN_EXPORT_DIR`: override export folder path
- `LINKEDIN_QA_DB`: override SQLite db path
- `LINKEDIN_QA_DATA_DIR`: override data directory

### Notes

- The index builds automatically on first run and whenever the export changes.
- Click "Rebuild Index" if you add new files and want an immediate refresh.
- First-time indexing can take a few minutes for large exports.
