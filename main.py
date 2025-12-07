# main.py
import os
import time
import logging
import traceback
import sys
from datetime import datetime, timedelta

import pandas as pd

# import jobspy module - we'll call scrape_jobs via jobspy.scrape_jobs so we can retry without location
import jobspy
from jobspy import scrape_jobs as _scrape_jobs  # keep existing import to avoid surprises
from resume_parser import extract_resume_keywords
from gspread_uploader import auth_gspread, fetch_existing_urls, append_new_rows

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ---------- Config ----------
JOB_SHEET_ID = os.environ.get("SHEET_ID")
SEARCH_LOCATION = os.environ.get("SEARCH_LOCATION", "Remote")
JOBS_PER_TERM = int(os.environ.get("JOBS_PER_TERM", "12"))
KEYWORDS_SOURCE = "resume.pdf"

TARGET_COLS = [
    "Date Scraped", "Platform", "Job Title", "Company", "Location",
    "Posted", "Salary", "Job URL", "Description", "Raw"
]

# ---------- Defensive monkeypatch for requests to add browser-like headers ----------
# This increases chance sites won't immediately 403 plain python requests.
import requests
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_old_requests_get = requests.get


def _patched_get(url, *args, **kwargs):
    # merge default headers with any provided headers
    headers = kwargs.pop("headers", {}) or {}
    merged = DEFAULT_HEADERS.copy()
    merged.update(headers)
    kwargs["headers"] = merged

    # simple retry wrapper (keeps things lightweight)
    max_attempts = 2
    for attempt in range(1, max_attempts + 1):
        try:
            r = _old_requests_get(url, *args, **kwargs)
            return r
        except requests.RequestException as e:
            logging.warning("requests.get attempt %d failed for %s: %s", attempt, url, e)
            if attempt == max_attempts:
                raise
            time.sleep(0.5)


# Apply monkeypatch so jobspy adapters using requests.get inherit these headers
requests.get = _patched_get

# ---------- Helper: call scrape_jobs while handling location-kw mismatch ----------
def call_scrape_jobs(search_terms, location=None, jobs_per_term=12):
    """
    Try calling scrape_jobs with location kwarg. If that errors due to adapter
    signature mismatch, retry without location. Returns whatever scrape_jobs returns.
    """
    try:
        logging.info("Running scrape_jobs with location=%s jobs_per_term=%s", location, jobs_per_term)
        return _scrape_jobs(search_terms, location=location, jobs_per_term=jobs_per_term)
    except TypeError as e:
        # This usually happens when some adapter functions don't accept location kwarg.
        logging.warning("scrape_jobs failed with TypeError when passing location: %s. Retrying without location.", e)
        try:
            return _scrape_jobs(search_terms, jobs_per_term=jobs_per_term)
        except Exception as e2:
            logging.error("scrape_jobs failed without location as well: %s", e2)
            raise


# ---------- Normalise / filter helpers (keep your logic) ----------
def normalize_and_filter(raw_jobs):
    """Minimal safe normalizer - adapt if your project has richer logic"""
    if raw_jobs is None:
        return pd.DataFrame(columns=TARGET_COLS)
    df = pd.DataFrame(raw_jobs)
    # keep only the target columns existing in df
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[TARGET_COLS]
    # dedupe by URL (safe)
    df["Job URL"] = df["Job URL"].astype(str).apply(lambda u: u.split("?")[0])
    df = df.drop_duplicates(subset=["Job URL"])
    return df


# ---------- Runner ----------
def main():
    try:
        logging.info("START: scraper run")
        if not JOB_SHEET_ID:
            logging.error("SHEET_ID env var missing. Set SHEET_ID and re-run.")
            sys.exit(1)

        # authenticate Google Sheets
        sh = auth_gspread()
        worksheet = sh.open_by_key(JOB_SHEET_ID).sheet1

        existing = fetch_existing_urls(worksheet)
        logging.info("Existing rows in sheet: %d", len(existing))

        # search terms: read from a file or hardcode; using sample keywords from README if present
        # For now: a few sample terms; adapt as your code expects.
        search_terms = ["aws", "cloud", "lambda", "automation", "through", "user", "engineer", "india"]
        # call the job scraper (robust wrapper)
        raw_jobs = call_scrape_jobs(search_terms, location=SEARCH_LOCATION, jobs_per_term=JOBS_PER_TERM)

        processed = normalize_and_filter(raw_jobs)

        if processed.empty:
            logging.info("No new jobs found.")
            return

        new_rows = processed[~processed["Job URL"].isin(existing)]
        if new_rows.empty:
            logging.info("No new unique jobs to append after filtering.")
            return

        # append and log clearly
        logging.info("ABOUT TO APPEND rows_count=%d", len(new_rows))
        appended = append_new_rows(worksheet, new_rows)
        logging.info("Appended rows count (function returned): %s", appended)

    except Exception as e:
        logging.error("UNCAUGHT: %s", e)
        logging.error(traceback.format_exc())
        sys.exit(1)
    finally:
        logging.info("END: scraper run")


if __name__ == "__main__":
    main()
