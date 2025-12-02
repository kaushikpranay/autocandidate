import os, time, logging
from datetime import datetime, timedelta
import pandas as pd
from jobspy import scrape_jobs
from resume_parser import extract_resume_keywords
from gspread_uploader import auth_gspread, fetch_existing_urls, append_new_rows

logging.basicConfig(level=logging.INFO)

JOB_SHEET_ID = os.environ.get("SHEET_ID")
SEARCH_LOCATION = os.environ.get("SEARCH_LOCATION", "Remote")
JOBS_PER_TERM = int(os.environ.get("JOBS_PER_TERM", "12"))
KEYWORDS_SOURCE = "resume.pdf"

TARGET_COLS = [
    "Date Scraped","Platform","Job Title","Company","Location",
    "Job Type","Salary Estimate","Job URL","Description Snippet","Application Status"
]

def build_search_terms():
    if os.path.exists(KEYWORDS_SOURCE):
        return extract_resume_keywords(KEYWORDS_SOURCE, top_n=8)
    return ["software engineer","python","java","cloud"]

def run_scraper(search_terms):
    all_jobs = []
    site_names = ["linkedin","indeed","glassdoor","zip_recruiter"]
    for term in search_terms:
        logging.info(f"Scraping term: {term}")
        try:
            jobs = scrape_jobs(
                site_name=site_names,
                search_term=term,
                google_search_term=f"{term} jobs in {SEARCH_LOCATION} since yesterday",
                location=SEARCH_LOCATION,
                results_wanted=JOBS_PER_TERM,
                hours_old=72
            )
            if jobs is not None and not jobs.empty:
                jobs['keyword_source'] = term
                all_jobs.append(jobs)
            time.sleep(6)
        except Exception as e:
            logging.warning(f"Scrape error for {term}: {e}")
    if not all_jobs:
        return pd.DataFrame()
    return pd.concat(all_jobs, ignore_index=True)

def normalize_and_filter(df):
    if df.empty:
        return df
    df = df.rename(columns={
        'site':'Platform','title':'Job Title','company':'Company',
        'job_url':'Job URL','location':'Location','description':'Description Snippet',
        'date_posted':'date_posted'
    })
    df['Date Scraped'] = datetime.utcnow().strftime("%Y-%m-%d")
    df['Application Status'] = "To Apply"
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = "N/A"
    try:
        cutoff = datetime.utcnow() - timedelta(hours=48)
        df['date_posted'] = pd.to_datetime(df.get('date_posted'), errors='ignore')
        df = df[(df['date_posted'] >= cutoff) | (df['date_posted'].isna())]
    except:
        pass
    return df[TARGET_COLS]

def main():
    gc = auth_gspread()
    sh = gc.open_by_key(JOB_SHEET_ID).sheet1

    existing = fetch_existing_urls(sh)
    search_terms = build_search_terms()
    raw = run_scraper(search_terms)
    processed = normalize_and_filter(raw)

    if processed.empty:
        logging.info("No fresh jobs found")
        return

    processed['Job URL'] = processed['Job URL'].apply(lambda u: str(u).split('?')[0])
    mask = ~processed['Job URL'].isin(existing)
    new_rows = processed[mask]

    appended = append_new_rows(sh, new_rows)
    logging.info(f"Appended {appended} rows")

if __name__ == "__main__":
    main()
