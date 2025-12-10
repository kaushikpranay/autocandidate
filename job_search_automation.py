
# job_search_automation.py (top)
import os, json

GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID', '{GOOGLE_SHEET_ID}')
# Service account JSON file path used by gspread code below
SERVICE_ACCOUNT_FILE = 'credentials.json'

# If you want to load the JSON from a string (workflow writes file), optionally:
if os.path.exists(SERVICE_ACCOUNT_FILE):
    pass
else:
    # The workflow will write credentials.json at runtime; this is just a safety fallback.
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if creds_json:
        with open(SERVICE_ACCOUNT_FILE, 'w', encoding='utf-8') as f:
            f.write(creds_json)
