# gspread_uploader.py
import os
import json
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread

logging.basicConfig(level=logging.INFO)

def auth_gspread():
    """
    Support two ways to provide credentials:
      1) GCP_CREDENTIALS_JSON  -> full service-account JSON string (good for GitHub secrets)
      2) GCP_CREDENTIALS_PATH  -> path to local service-account JSON file (local dev)
    Returns: an authorized gspread client (gspread.client.Client)
    """
    creds_json = os.environ.get("GCP_CREDENTIALS_JSON")
    creds_path = os.environ.get("GCP_CREDENTIALS_PATH")

    creds_dict = None
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
        except Exception as e:
            raise SystemExit(f"Failed to parse GCP_CREDENTIALS_JSON: {e}")
    elif creds_path:
        if not os.path.exists(creds_path):
            raise SystemExit(f"GCP_CREDENTIALS_PATH set but file not found: {creds_path}")
        try:
            with open(creds_path, "r", encoding="utf-8") as f:
                creds_dict = json.load(f)
        except Exception as e:
            raise SystemExit(f"Failed to read service account JSON from path: {e}")
    else:
        raise SystemExit("Missing credentials: set either GCP_CREDENTIALS_JSON or GCP_CREDENTIALS_PATH env var.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def fetch_existing_urls(sheet):
    """
    sheet: a gspread.Spreadsheet instance or a worksheet object (we expect worksheet)
    Returns a set of urls that are already present in the sheet (safe to compare).
    """
    try:
        recs = sheet.get_all_records()
    except Exception as e:
        logging.error("Failed to read sheet records: %s", e)
        return set()

    if not recs:
        return set()
    df = pd.DataFrame(recs).astype(str)
    if "Job URL" in df.columns:
        return set(df["Job URL"].apply(lambda x: x.split("?")[0]).tolist())
    return set()

def append_new_rows(sheet, df_new):
    """
    sheet: gspread worksheet (the worksheet object returned by open_by_key(...).sheet1)
    df_new: pandas DataFrame with rows to append
    Returns: number of appended rows (int)
    """
    if df_new is None or df_new.empty:
        logging.info("append_new_rows called with empty dataframe.")
        return 0

    try:
        rows = df_new.values.tolist()
        logging.info("append_new_rows: attempting to append %d rows", len(rows))
        res = sheet.append_rows(rows, value_input_option='USER_ENTERED')
        logging.info("append_new_rows: response=%s", res)
        return len(rows)
    except Exception as e:
        logging.error("Failed to append rows to sheet: %s", e)
        logging.error("Traceback:", exc_info=True)
        return 0
