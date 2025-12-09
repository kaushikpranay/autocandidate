# jobspy.py
# Minimal dependencies: requests, bs4 (BeautifulSoup), pandas
# Optional: xml.etree for RSS parsing (stdlib)
# NOTE: This is a reference implementation focused on clarity and safety.
import time
import requests
import logging
import json
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pandas as pd
import xml.etree.ElementTree as ET

# polite defaults
USER_AGENT = "jobspy-bot/1.0 (+https://yourproject.example) Sparky-contact"
RATE_LIMIT_S = 1.0  # seconds between requests per site (tweak as needed)
REQUEST_TIMEOUT = 15

# Sites known to be paywalled or require login
PAYWALLED_SITES = {
    "flexjobs": "requires login/paywall",
    # Add others if you verify paywall
}

# Map canonical site keys to friendly names & homepage (for your output)
SITE_META = {
    "remotive": ("Remotive", "https://remotive.com"),
    "remoteok": ("RemoteOK", "https://remoteok.com"),
    "weworkremotely": ("We Work Remotely", "https://weworkremotely.com"),
    "pangian": ("Pangian", "https://pangian.com"),
    "simplyhired": ("SimplyHired", "https://www.simplyhired.com"),
    "jobspresso": ("Jobspresso", "https://jobspresso.co"),
    "outsourcely": ("Outsourcely", "https://www.outsourcely.com"),
    "toptal": ("Toptal", "https://www.toptal.com"),
    "skipthedrive": ("Skip The Drive", "https://www.skipthedrive.com"),
    "nodesk": ("NoDesk", "https://nodesk.co"),
    "remotehabits": ("RemoteHabits", "https://remotehabits.com"),
    "remotive_site": ("Remotive site", "https://remotive.com"),
    "remote4me": ("Remote4Me", "https://remote4me.com"),
    "remotees": ("Remotees", "https://remotees.com"),
    "europeremotely": ("EuropeRemotely", "https://europeremotely.com"),
    "remoteokeu": ("Remote OK Europe (link)", "https://inkd.in/gr4C-mjp"),
    "remoteofasia": ("Remote of Asia", "https://inkd.in/ghrA_z9u"),
    "angel": ("AngelList / Wellfound", "https://wellfound.com"),
    "linkedin": ("LinkedIn", "https://www.linkedin.com"),
    "freelancer": ("Freelancer", "https://www.freelancer.com"),
    "workingnomads": ("Working Nomads", "https://www.workingnomads.com"),
    "virtualvocations": ("Virtual Vocations", "https://www.virtualvocations.com"),
    "remotefreelance": ("Remote Freelance", "https://remotefreelance.com"),
    "remoterocketship": ("Remote Rocketship", "https://inkd.in/gS2nRtV3"),
}

# Unified columns expected by main.py TARGET_COLS (keep them)
TARGET_COLS = [
    "Date Scraped","Platform","Job Title","Company","Location",
    "Job Type","Salary Estimate","Job URL","Description Snippet","Application Status"
]

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

def _safe_get(url, params=None, headers=None):
    try:
        r = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        time.sleep(RATE_LIMIT_S)
        r.raise_for_status()
        return r
    except Exception as e:
        logging.warning(f"HTTP GET failed for {url}: {e}")
        return None

def _rows_from_items(items, platform_name):
    rows = []
    for it in items:
        rows.append({
            "Date Scraped": datetime.utcnow().strftime("%Y-%m-%d"),
            "Platform": platform_name,
            "Job Title": it.get("title","N/A"),
            "Company": it.get("company","N/A"),
            "Location": it.get("location","N/A"),
            "Job Type": it.get("type","N/A"),
            "Salary Estimate": it.get("salary","N/A"),
            "Job URL": it.get("url","N/A"),
            "Description Snippet": it.get("snippet","N/A"),
            "Application Status": "To Apply",
            # include raw posted date too for filtering; not part of TARGET_COLS
            "date_posted": it.get("date_posted")
        })
    return rows

# --------------------------
# Per-site adapters (keep them lightweight)
# --------------------------

def fetch_remotive(search_term=None, location=None, results_wanted=20):
    """
    Uses Remotive public API. Returns list of dicts: title, company, location, url, snippet, date_posted
    Docs: https://remotive.com/remote-jobs/api
    """
    api = "https://remotive.com/api/remote-jobs"
    params = {}
    if search_term:
        params["search"] = search_term
    r = _safe_get(api, params=params)
    if not r:
        return []
    try:
        payload = r.json()
        jobs = payload.get("jobs", [])[:results_wanted]
        items = []
        for j in jobs:
            items.append({
                "title": j.get("title"),
                "company": j.get("company_name"),
                "location": j.get("candidate_required_location") or location or "Remote",
                "url": j.get("url"),
                "snippet": j.get("description")[:300] if j.get("description") else "",
                "date_posted": j.get("publication_date")  # ISO string
            })
        return items
    except Exception as e:
        logging.warning("Failed parse Remotive JSON: %s", e)
        return []

def fetch_remoteok(search_term=None, results_wanted=20):
    """
    RemoteOK exposes a JSON feed: https://remoteok.com/remote-jobs.json or /api
    Note: their terms require direct linking back. Respect that.
    """
    api = "https://remoteok.com/remote-jobs"
    r = _safe_get(api)
    if not r:
        return []
    try:
        # remoteok sometimes returns JS-like content; attempt json parse
        raw = r.text
        # strip any leading non-json tokens
        raw_json = raw
        try:
            payload = json.loads(raw_json)
        except:
            # try /api or /api?format=json
            alt = _safe_get("https://remoteok.com/api")
            if alt:
                payload = alt.json()
            else:
                return []
        items = []
        for job in payload.get("jobs", payload)[:results_wanted]:
            title = job.get("position") or job.get("title")
            items.append({
                "title": title,
                "company": job.get("company"),
                "location": job.get("location") or "Remote",
                "url": job.get("url") or job.get("apply_url") or job.get("link"),
                "snippet": (job.get("description") or "")[:300],
                "date_posted": job.get("date") or job.get("time") or job.get("created_at")
            })
        return items
    except Exception as e:
        logging.warning("Failed parse RemoteOK payload: %s", e)
        return []

def fetch_weworkremotely_rss(search_term=None, results_wanted=20):
    """
    WeWorkRemotely provides RSS feed: https://weworkremotely.com/remote-jobs.rss
    We'll parse and filter by title/description.
    """
    feed_url = "https://weworkremotely.com/remote-jobs.rss"
    r = _safe_get(feed_url)
    if not r:
        return []
    try:
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:results_wanted]:
            title = item.findtext("title")
            link = item.findtext("link")
            desc = item.findtext("description") or ""
            pub = item.findtext("pubDate")
            items.append({
                "title": title,
                "company": "N/A",
                "location": "Remote",
                "url": link,
                "snippet": desc[:300],
                "date_posted": pub
            })
        return items
    except Exception as e:
        logging.warning("Failed parse WWR RSS: %s", e)
        return []

def fetch_simplyhired(search_term=None, location=None, results_wanted=20):
    """
    Lightweight HTML parsing for SimplyHired.
    NOTE: site structure changes frequently. Use this adapter as best-effort.
    """
    base = "https://www.simplyhired.com"
    q = f"{search_term} jobs" if search_term else "software jobs"
    url = f"{base}/search?q={requests.utils.quote(q)}&l={requests.utils.quote(location or '')}"
    r = _safe_get(url)
    if not r:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    posts = soup.select(".SerpJob-jobCard") or soup.select("div.card") or []
    for p in posts[:results_wanted]:
        try:
            a = p.find("a", href=True)
            link = urljoin(base, a["href"]) if a else None
            title = a.get_text(strip=True) if a else p.find("h2").get_text(strip=True)
            company = p.select_one(".jobposting-company") or p.select_one(".SerpJob-link")
            comp = company.get_text(strip=True) if company else "N/A"
            desc = p.get_text(strip=True)[:300]
            items.append({
                "title": title, "company": comp, "location": location or "N/A",
                "url": link, "snippet": desc, "date_posted": None
            })
        except Exception:
            continue
    return items

# Minimal fallback generic HTML search (used for simple sites with public listings)
def fetch_generic_html_list(url, css_selector_item, title_sel, link_sel, company_sel=None, results_wanted=20):
    r = _safe_get(url)
    if not r:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    items = []
    posts = soup.select(css_selector_item)[:results_wanted]
    for p in posts:
        try:
            title_el = p.select_one(title_sel)
            link_el = p.select_one(link_sel)
            title = title_el.get_text(strip=True) if title_el else "N/A"
            link = urljoin(url, link_el.get("href")) if link_el and link_el.get("href") else None
            company = p.select_one(company_sel).get_text(strip=True) if company_sel and p.select_one(company_sel) else "N/A"
            snippet = p.get_text(strip=True)[:300]
            items.append({
                "title": title, "company": company, "location": "Remote", "url": link, "snippet": snippet, "date_posted": None
            })
        except Exception:
            continue
    return items

# Adapter registry
ADAPTERS = {
    "remotive": fetch_remotive,
    "remoteok": fetch_remoteok,
    "weworkremotely": fetch_weworkremotely_rss,
    "simplyhired": fetch_simplyhired,
    # for other sites, use generic or TODO placeholders
}

# Sites that we will report as "requires login/paywall"
PAYWALLED_OR_LOGIN = set(["flexjobs"])

def scrape_jobs(site_name=None, search_term=None, google_search_term=None,
                location=None, results_wanted=12, hours_old=24):
    """
    Unified entry point.
    - site_name may be a list of site keys or a single key.
    - returns pandas.DataFrame with columns matching TARGET_COLS and with a 'date_posted' column used for filtering.
    """
    if isinstance(site_name, str):
        sites = [site_name]
    else:
        sites = site_name or list(ADAPTERS.keys())

    all_rows = []
    for s in sites:
        key = s.lower()
        friendly = SITE_META.get(key, (key, None))[0]
        if key in PAYWALLED_OR_LOGIN:
            logging.info(f"{friendly} marked as paywalled/login-only; skipping. (flagged)")
            continue
        adapter = ADAPTERS.get(key)
        if adapter is None:
            logging.info(f"No adapter for {key}; marking as skipped (TODO adapter).")
            continue
        try:
            items = adapter(search_term=search_term, location=location, results_wanted=results_wanted)
            rows = _rows_from_items(items, friendly)
            all_rows.extend(rows)
        except Exception as e:
            logging.warning("Adapter error for %s: %s", key, e)

    if not all_rows:
        return pd.DataFrame(columns=TARGET_COLS + ["date_posted"])

    df = pd.DataFrame(all_rows)
    # Normalize some columns
    if "Job URL" in df.columns:
        df["Job URL"] = df["Job URL"].astype(str)
    # ensure Date Scraped present
    if "Date Scraped" not in df.columns:
        df["Date Scraped"] = datetime.utcnow().strftime("%Y-%m-%d")
    # Filter last hours_old hours by date_posted when possible
    try:
        cutoff = datetime.utcnow() - timedelta(hours=int(hours_old))
        # try parsing date_posted to datetime for rows that have it (best-effort)
        def parse_date(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            try:
                return datetime.fromisoformat(val.replace("Z",""))
            except:
                try:
                    # fallback parse common formats
                    return datetime.strptime(val, "%a, %d %b %Y %H:%M:%S %Z")
                except:
                    return None
        df['date_parsed'] = df['date_posted'].apply(parse_date)
        keep = (df['date_parsed'].isna()) | (df['date_parsed'] >= cutoff)
        df = df[keep]
        # drop helper col (but keep original date_posted)
        df = df.drop(columns=["date_parsed"], errors='ignore')
    except Exception:
        pass

    # fill target columns if missing
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = "N/A"

    # Reorder and return
    out = df[TARGET_COLS + ["date_posted"]]
    return out
# ---- BEGIN PATCH: robust fallback scrape_jobs override ----
# Defensive wrapper to avoid treating every search word as an adapter name.
# This will override the existing scrape_jobs function with a safer implementation.
import importlib
import logging
import os

LOG = logging.getLogger(__name__)

# Edit this list to include the adapter modules your project actually provides.
# If your adapters live under a package (e.g. autocandidate.adapters.<name>), add them there later.
KNOWN_ADAPTERS = {"indeed", "naukri", "linkedin", "glassdoor", "monster", "fake"}
DEFAULT_ADAPTERS = ["remotive","remoteok"]

def _import_adapter_module(name):
    candidates = [
        f"adapters.{name}",
        f"{name}_adapter",
        f"autocandidate.adapters.{name}",
        f"autocandidate.{name}",
        name
    ]
    for cand in candidates:
        try:
            mod = importlib.import_module(cand)
            LOG.debug("Imported adapter module %s as candidate %s", cand, name)
            return mod
        except Exception:
            continue
    LOG.debug("No import succeeded for adapter %s", name)
    return None

def _call_adapter_search(mod, query, location, limit):
    """
    Try common adapter API shapes. Return list of jobs or [].
    """
    try:
        if hasattr(mod, "search"):
            return mod.search(query=query or None, location=location, limit=limit)
        if hasattr(mod, "scrape"):
            return mod.scrape(query or None, location=location, limit=limit)
        # try direct callable (module implements __call__ or function named run)
        if hasattr(mod, "run"):
            return mod.run(query=query or None, location=location, limit=limit)
        if callable(mod):
            return mod(query=query or None, location=location, limit=limit)
    except Exception:
        LOG.exception("Adapter %s raised during search", getattr(mod, "__name__", str(mod)))
    return []

def _normalize_jobs(jobs):
    if not jobs:
        return []
    out = []
    seen = set()
    for j in jobs:
        # normalize a key to dedupe
        url = None
        if isinstance(j, dict):
            url = j.get("url") or j.get("link")
            key = url or (j.get("title"), j.get("company"))
        else:
            # not dict, fallback to string representation
            key = str(j)
        if key in seen:
            continue
        seen.add(key)
        out.append(j)
    return out

def scrape_jobs(*args, **kwargs):
    """
    Robust fallback scraper.
    Signature compatible with older callers: accepts kwargs like location, jobs_per_term, query, terms.
    Behavior:
      - Ignore brittle token->adapter mapping.
      - Use DEFAULT_ADAPTERS if no explicit adapters specified.
      - Call adapters with (query, location, limit).
    """
    # try to preserve compatibility with callers
    location = kwargs.get("location") or kwargs.get("loc") or os.environ.get("SEARCH_LOCATION")
    # jobs_per_term may be passed as jobs_per_term, limit, or JOBS_PER_TERM env var
    limit = kwargs.get("jobs_per_term") or kwargs.get("limit") or os.environ.get("JOBS_PER_TERM") or 12
    try:
        limit = int(limit)
    except Exception:
        limit = 12

    # Query/terms may be provided as 'query' or 'terms' or 'q'
    query = kwargs.get("query") or kwargs.get("terms") or kwargs.get("q") or ""

    LOG.info("scrape_jobs (fallback) called with location=%r, query=%r, limit=%r", location, query, limit)

    adapters_to_try = []

    # If caller explicitly asked for adapters via kwargs['adapters'] (list/string), respect that
    explicit = kwargs.get("adapters") or kwargs.get("adapter") or kwargs.get("adapters_list")
    if explicit:
        if isinstance(explicit, (list, tuple)):
            adapters_to_try = [str(x).lower() for x in explicit if str(x).strip()]
        else:
            adapters_to_try = [str(s).strip().lower() for s in str(explicit).split(",") if s.strip()]

    # if still empty, use defaults
    if not adapters_to_try:
        adapters_to_try = DEFAULT_ADAPTERS.copy()

    LOG.debug("Adapters to try: %s", adapters_to_try)

    all_jobs = []
    for name in adapters_to_try:
        mod = _import_adapter_module(name)
        if not mod:
            LOG.warning("Adapter module for %s not found; skipping", name)
            continue
        jobs = _call_adapter_search(mod, query=query, location=location, limit=limit)
        if jobs:
            all_jobs.extend(jobs)

    unique = _normalize_jobs(all_jobs)
    LOG.info("scrape_jobs (fallback) returning %d jobs", len(unique))
    return unique

# ---- END PATCH ----
