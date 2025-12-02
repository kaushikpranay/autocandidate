import os, json, logging
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread

def auth_gspread():
    creds_json = os.environ.get("GCP_CREDENTIALS_JSON")
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def fetch_existing_urls(sheet):
    recs = sheet.get_all_records()
    if not recs:
        return set()
    df = pd.DataFrame(recs).astype(str)
    if "Job URL" in df.columns:
        return set(df["Job URL"].apply(lambda x: x.split("?")[0]).tolist())
    return set()

def append_new_rows(sheet, df_new):
    if df_new.empty:
        return 0
    sheet.append_rows(df_new.values.tolist(), value_input_option='USER_ENTERED')
    return len(df_new)
