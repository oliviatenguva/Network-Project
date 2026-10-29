"""
DS 3022 Data Project 2: Open Source Package Health Pipeline
Package set A: numpy, networkx, statsmodels

One Prefect flow runs six tasks in order:
  1. Extract  - GitHub API -> data/raw/*.json
  2. Load     - data/raw/*.json -> packages.duckdb (raw_* tables)
  3. dbt      - build staging/fact/mart models and run tests
  4. Validate - Python checks on the whole pipeline (no dbt)
  5. Model    - health scores and ranking
  6. Plot     - PNG charts saved to plots/
"""

# ---------- Imports ----------
import json                                   # read/write the landing files
import logging                                # Python's built-in logging, used for the per-task log files
import os                                     # environment variables + os.replace for atomic writes
import shutil                                 # shutil.rmtree to delete data/raw/ on a fresh start
import time                                   # time.sleep() and time.time() for rate limiting
from datetime import datetime, timezone       # timestamps (GitHub uses UTC)
from pathlib import Path                      # file paths that work on Windows and Mac/Linux

import duckdb                                 # read _load_state watermarks (Task 2 creates it)
import httpx                                  # HTTP client for calling the GitHub API
import pandas as pd                           # batch of rows handed to DuckDB in Task 2
import matplotlib
matplotlib.use("Agg")                         # draw straight to PNG files; no GUI window needed
import matplotlib.pyplot as plt
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NONE       # turns off Prefect's result caching for a task
from prefect.tasks import exponential_backoff # retry delays that grow each attempt (10s, 20s, 40s...)
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings  # runs `dbt build` inside a task


# ---------- Config ----------
# Package set A: one large repo (numpy) and two smaller ones
REPOS = ["numpy/numpy", "networkx/networkx", "statsmodels/statsmodels"]

# Fixed start of the analysis window. There is no end date: the window runs to "now".
WINDOW_START = "2024-01-01T00:00:00Z"

# All paths are relative to this file, so the flow works from any folder.
PROJECT_DIR = Path(__file__).parent
RAW_DIR = PROJECT_DIR / "data" / "raw"        # landing zone for raw JSON pages (gitignored)
LOADED_MARKER = RAW_DIR / "_LOADED"           # created by Task 2 once everything is in DuckDB
DB_PATH = PROJECT_DIR / "packages.duckdb"     # DuckDB database file (gitignored)
LOG_DIR = PROJECT_DIR / "logs"                # one .log file per task (gitignored)
PLOT_DIR = PROJECT_DIR / "plots"              # output PNGs (these ARE committed)


# ---------- Logging helper ----------
class TaskLog:
    """
    Sends each message to TWO places, as the assignment requires:
      1. The Prefect logger, so it shows in the terminal and the Prefect UI.
      2. A task-specific file, logs/<name>.log.
    """

    def __init__(self, name: str):
        # Prefect's logger. Only works inside a running task or flow.
        self.prefect = get_run_logger()

        # A normal Python logger with a unique name per task, e.g. "pipeline.1_extract"
        self.file = logging.getLogger(f"pipeline.{name}")

        # Loggers are global in Python. If a handler is already attached, don't
        # add another one, or every line gets written twice.
        if not self.file.handlers:
            LOG_DIR.mkdir(exist_ok=True)
            handler = logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self.file.addHandler(handler)
            self.file.setLevel(logging.INFO)
            self.file.propagate = False   # don't also pass messages up to the root logger

    # Each method writes the same message to both destinations.
    def info(self, msg):
        self.prefect.info(msg)
        self.file.info(msg)

    def warning(self, msg):
        self.prefect.warning(msg)
        self.file.warning(msg)

    def error(self, msg):
        self.prefect.error(msg)
        self.file.error(msg)


# ---------- GitHub HTTP helper ----------
API = "https://api.github.com"
RATE_LIMIT_BUFFER = 20   # sleep when this few requests are left, so we never actually hit 0
REQUEST_COUNT = 0        # API odometer: total requests made during this flow run
CLIENT = httpx.Client(timeout=30)  # one shared connection, reused for every request (much faster than httpx.get each time)


class TransientHTTPError(Exception):
    """
    Errors that will probably go away if we wait and try again: 5xx server
    errors, secondary rate limits, truncated JSON. Raising this tells
    Prefect "this one is worth retrying".
    """


def retry_if_transient(task, task_run, state) -> bool:
    """
    Prefect calls this after a task fails to decide whether to retry it.
    state.result() re-raises the exception the task failed with.
      - TransientHTTPError or a network error -> True (retry)
      - Anything else, e.g. a ValueError from a 404 -> False (fail right away)
    """
    try:
        state.result()
    except (TransientHTTPError, httpx.TransportError):
        return True
    except Exception:
        return False
    return False


def sleep_until(reset_epoch: int, log: TaskLog, reason: str) -> None:
    """Sleep until GitHub's rate-limit window resets (X-RateLimit-Reset, epoch seconds UTC)."""
    wait = max(0, reset_epoch - time.time()) + 5  # +5s margin in case clocks differ slightly
    reset_str = datetime.fromtimestamp(reset_epoch, timezone.utc).strftime("%H:%M:%S")
    log.warning(f"{reason}: sleeping {wait:.0f}s until {reset_str} UTC")
    time.sleep(wait)


@task(
    retries=5,                                                   # up to 5 retries...
    retry_delay_seconds=exponential_backoff(backoff_factor=10),  # ...waiting longer each time
    retry_condition_fn=retry_if_transient,                       # ...but only for temporary errors
    cache_policy=NONE,  # always make a fresh request; also keeps Prefect from hashing the token
)
def fetch_page(url: str, token: str) -> tuple:
    """
    Make ONE GET request to GitHub and return (json_body, next_page_url).
    next_page_url is None when this is the last page. Every API call goes
    through here, so rate limiting, errors, and counting live in one place.
    """
    global REQUEST_COUNT
    log = TaskLog("1_extract")

    headers = {
        "Authorization": f"Bearer {token}",           # authenticated: 5,000 requests/hour
        "Accept": "application/vnd.github+json",      # GitHub's recommended media type
        "X-GitHub-Api-Version": "2022-11-28",         # pin the API version so responses don't change
    }

    # --- Make the request (reusing the shared client) ---
    try:
        resp = CLIENT.get(url, headers=headers)
    except httpx.TransportError as e:
        # Timeout, DNS failure, dropped connection. Log the URL, then re-raise to retry.
        log.warning(f"Network error on {url}: {e}")
        raise

    REQUEST_COUNT += 1  # count every request that reached GitHub, success or not

    # --- Rate-limit headers (present on every response) ---
    remaining = int(resp.headers.get("X-RateLimit-Remaining", RATE_LIMIT_BUFFER + 1))
    reset = int(resp.headers.get("X-RateLimit-Reset", time.time()))

    # --- 403/429: rate limited ---
    if resp.status_code in (403, 429):
        if "Retry-After" in resp.headers:
            # Secondary rate limit: GitHub tells us exactly how long to wait.
            wait = int(resp.headers["Retry-After"])
            log.warning(f"Secondary rate limit on {url}: sleeping {wait}s")
            time.sleep(wait)
        elif remaining == 0:
            # Primary limit: hourly budget used up. Wait for the reset.
            sleep_until(reset, log, "Primary rate limit hit")
        else:
            # Secondary limit with no Retry-After. GitHub says wait at least a minute.
            log.warning(f"{resp.status_code} with no Retry-After on {url}: sleeping 60s")
            time.sleep(60)
        raise TransientHTTPError(f"{resp.status_code} on {url}")  # Prefect retries the same URL

    # --- 5xx: GitHub's server had a problem. Usually temporary, so retry. ---
    if resp.status_code >= 500:
        raise TransientHTTPError(f"{resp.status_code} server error on {url}")

    # --- Other 4xx (401 bad token, 404, 422): our mistake. Don't retry. ---
    if resp.status_code >= 400:
        raise ValueError(f"{resp.status_code} on {url}: {resp.text[:200]}")

    # --- Proactive: nearly out of budget? Sleep now, before we get blocked. ---
    if remaining <= RATE_LIMIT_BUFFER:
        sleep_until(reset, log, f"Only {remaining} requests left")

    # --- Parse the body ---
    try:
        body = resp.json()
    except ValueError as e:
        raise TransientHTTPError(f"Unparseable JSON from {url}") from e

    # --- Pagination: the Link header's rel="next" URL, or None on the last page ---
    next_url = resp.links.get("next", {}).get("url")
    return body, next_url


# ---------- Landing-file helpers ----------
def utc_now_str() -> str:
    """Current UTC time in GitHub's format, e.g. 2026-10-25T18:40:41Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json_atomic(path: Path, obj: dict) -> None:
    """
    Write to a .tmp file first, then rename it into place. os.replace is
    atomic, so if the run crashes mid-write, only the .tmp file is left
    and page_XXXX.json is either complete or doesn't exist.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(obj), encoding="utf-8")
    os.replace(tmp, path)


def read_landing(path: Path) -> dict | None:
    """Return a landed page, or None if it's missing or corrupt (so it gets re-fetched)."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def paginate(repo: str, endpoint: str, first_url: str, out_dir: Path,
             token: str, stop_before: str | None = None) -> tuple:
    """
    Walk every page of one endpoint, writing one landing file per page.
    RESUME: if page_N.json is already on disk, read it (records + next_url)
    instead of calling the API, so an interrupted run costs no extra quota.
    stop_before: for PRs sorted by updated desc, stop once a page's last
    PR was updated before this timestamp (the rest are older).
    Returns (records, pages_fetched, pages_skipped).
    """
    records, fetched, skipped = [], 0, 0
    url, page = first_url, 1

    while url:
        path = out_dir / f"page_{page:04d}.json"
        landed = read_landing(path)

        if landed is not None:
            # Already on disk from an interrupted run: reuse it.
            body, next_url = landed["data"], landed.get("next_url")
            skipped += 1
        else:
            body, next_url = fetch_page(url, token)
            write_json_atomic(path, {
                "repo": repo,
                "endpoint": endpoint,
                "url": url,
                "page": page,
                "fetched_at": utc_now_str(),
                "next_url": next_url,   # extra metadata so resume can continue the chain
                "data": body,           # the unmodified GitHub response
            })
            fetched += 1

        records.extend(body)

        # ISO-8601 UTC strings compare correctly as plain strings.
        if stop_before and body and body[-1]["updated_at"] < stop_before:
            break

        url, page = next_url, page + 1

    return records, fetched, skipped


def scalar(con, sql: str):
    """
    Run a query that returns one value. fetchall() reads the whole result;
    DuckDB 2.0 refuses the next statement while a result is only partly read
    (which is what fetchone() leaves behind).
    """
    return con.execute(sql).fetchall()[0][0]


def get_watermarks() -> dict:
    """
    Read _load_state from DuckDB: {(repo, endpoint): newest updated_at loaded}.
    Returns {} on the first run (no database or no table yet), meaning a full pull.
    """
    if not DB_PATH.exists():
        return {}
    with duckdb.connect(str(DB_PATH)) as con:   # default mode, same as every other connection
        has_table = scalar(con, "SELECT count(*) FROM information_schema.tables WHERE table_name = '_load_state'")
        if not has_table:
            return {}
        rows = con.execute("SELECT repo, endpoint, max_updated_at FROM _load_state").fetchall()
    return {(repo, ep): ts.strftime("%Y-%m-%dT%H:%M:%SZ") for repo, ep, ts in rows if ts}


def extract_repo(repo: str, token: str, watermarks: dict, log: TaskLog) -> dict:
    """Pull all four endpoints for one repo. Returns record counts per endpoint."""
    repo_dir = RAW_DIR / repo.replace("/", "__")   # numpy/numpy -> numpy__numpy
    counts = {}

    def report(endpoint, records, fetched, skipped):
        counts[endpoint] = len(records)
        log.info(f"{repo} | {endpoint}: {len(records)} records "
                 f"({fetched} pages fetched, {skipped} pages already on disk)")

    # --- Commits: the API filters by date with `since` ---
    since = watermarks.get((repo, "commits"), WINDOW_START)
    url = f"{API}/repos/{repo}/commits?since={since}&per_page=100"
    report("commits", *paginate(repo, "commits", url, repo_dir / "commits", token))

    # --- Pull requests: no date filter, so sort by updated desc and stop early ---
    pr_cutoff = watermarks.get((repo, "pulls"), WINDOW_START)
    url = f"{API}/repos/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100"
    pulls, fetched, skipped = paginate(repo, "pulls", url, repo_dir / "pulls", token,
                                       stop_before=pr_cutoff)
    report("pulls", pulls, fetched, skipped)

    # --- PR reviews: one call per PR created in the window ---
    # The updated_at filter drops the extra PRs on the last page that fall
    # before the cutoff. On an incremental run, only new or updated PRs remain.
    review_prs = sorted({p["number"] for p in pulls
                         if p["created_at"] >= WINDOW_START and p["updated_at"] >= pr_cutoff})
    log.info(f"{repo} | fetching reviews for {len(review_prs)} PRs")
    all_reviews, r_fetched, r_skipped = [], 0, 0
    for i, number in enumerate(review_prs, start=1):
        url = f"{API}/repos/{repo}/pulls/{number}/reviews?per_page=100"
        recs, f, s = paginate(repo, "pr_reviews", url,
                              repo_dir / "pr_reviews" / str(number), token)
        all_reviews.extend(recs)
        r_fetched, r_skipped = r_fetched + f, r_skipped + s
        if i % 250 == 0:  # progress update so long runs don't look frozen
            log.info(f"{repo} | reviews progress: {i}/{len(review_prs)} PRs "
                     f"| requests so far: {REQUEST_COUNT}")
    report("pr_reviews", all_reviews, r_fetched, r_skipped)

    # --- Releases: small, so always fetch in full ---
    url = f"{API}/repos/{repo}/releases?per_page=100"
    report("releases", *paginate(repo, "releases", url, repo_dir / "releases", token))

    return counts


# ---------- Load helpers (Task 2) ----------
# endpoint folder -> (raw table, id type, natural key, updated_at)
# Commits and reviews have no updated_at, so we use the closest date they do have.
# For commits that is the committer date, the same field GitHub's `since` filters on.
RAW_TABLES = {
    "commits":    ("raw_commits",       "VARCHAR", lambda r: r["sha"],    lambda r: r["commit"]["committer"]["date"]),
    "pulls":      ("raw_pull_requests", "BIGINT",  lambda r: r["number"], lambda r: r["updated_at"]),
    "pr_reviews": ("raw_pr_reviews",    "BIGINT",  lambda r: r["id"],     lambda r: r.get("submitted_at")),
    "releases":   ("raw_releases",      "BIGINT",  lambda r: r["id"],     lambda r: r.get("published_at") or r["created_at"]),
}

LOAD_STATE_DDL = """
    CREATE TABLE IF NOT EXISTS _load_state (
        repo           VARCHAR,
        endpoint       VARCHAR,
        max_updated_at TIMESTAMP,   -- newest updated_at loaded; Task 1's watermark
        loaded_at      TIMESTAMP,   -- when the load finished (UTC)
        PRIMARY KEY (repo, endpoint)
    )
"""


def raw_table_ddl(table: str, id_type: str) -> str:
    """Same six columns for every raw table; everything else stays inside payload."""
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            repo        VARCHAR,
            id          {id_type},
            updated_at  TIMESTAMP,
            fetched_at  TIMESTAMP,
            source_file VARCHAR,
            payload     JSON,
            PRIMARY KEY (repo, id)
        )
    """


def read_endpoint(endpoint: str) -> pd.DataFrame:
    """
    Read every landing file for one endpoint (all repos) into one row per record.
    The same record can appear on two pages (e.g. a PR updated mid-pagination
    moves pages), so keep only the most recently fetched copy of each (repo, id).
    """
    _, _, get_id, get_updated = RAW_TABLES[endpoint]
    rows = []
    # ** also matches zero folders, so this finds pulls/page_*.json AND pr_reviews/<n>/page_*.json
    for path in sorted(RAW_DIR.glob(f"*/{endpoint}/**/page_*.json")):
        landed = read_landing(path)
        if landed is None:
            raise ValueError(f"Corrupt landing file {path}; rerun the flow so Task 1 re-fetches it")
        source = path.relative_to(PROJECT_DIR).as_posix()
        for rec in landed["data"]:
            try:
                rows.append({
                    "repo": landed["repo"],
                    "id": get_id(rec),
                    "updated_at": get_updated(rec),
                    "fetched_at": landed["fetched_at"],
                    "source_file": source,
                    "payload": json.dumps(rec),
                })
            except (KeyError, TypeError) as e:
                raise ValueError(f"Unexpected record shape in {source}: missing {e}") from e

    df = pd.DataFrame(rows, columns=["repo", "id", "updated_at", "fetched_at", "source_file", "payload"])
    return df.sort_values("fetched_at").drop_duplicates(["repo", "id"], keep="last")


def upsert(con, table: str, batch: pd.DataFrame) -> None:
    """
    INSERT OR REPLACE on the (repo, id) primary key: a record already in the
    table is overwritten, never duplicated, so reloading the same files is a no-op.
    """
    if batch.empty:
        return
    con.register("batch", batch)
    con.execute(f"""
        INSERT OR REPLACE INTO {table}
        SELECT repo, id, updated_at::TIMESTAMP, fetched_at::TIMESTAMP, source_file, payload::JSON
        FROM batch
    """)
    con.unregister("batch")


# ---------- Validation helpers (Task 4) ----------
# endpoint folder -> (staging model, rows of the raw table that staging keeps).
# Staging drops PENDING reviews and draft releases on purpose, so the raw count
# we compare against excludes exactly those rows.
STAGING = {
    "commits":    ("stg_commits",       "true"),
    "pulls":      ("stg_pull_requests", "true"),
    "pr_reviews": ("stg_pr_reviews",    "payload->>'$.state' <> 'PENDING'"),
    "releases":   ("stg_releases",      "not coalesce((payload->>'$.draft')::boolean, false)"),
}


def connect_db():
    """
    Open packages.duckdb with UTC as the session time zone (matches dbt's profile).
    Always the default read-write mode: dbt runs in this same process and keeps its
    connection open, and DuckDB refuses a second connection to the same file in a
    different mode (e.g. read_only=True).
    """
    con = duckdb.connect(str(DB_PATH))
    con.execute("SET TimeZone = 'UTC'")
    return con


def check_completeness(con, log: TaskLog) -> list:
    """Return a list of failure messages (empty = every check passed)."""
    failures = []

    for endpoint, (table, _, _, _) in RAW_TABLES.items():
        stg, kept = STAGING[endpoint]

        # 1. Every distinct (repo, id) in data/raw/ is in the raw table.
        file_ids = read_endpoint(endpoint)[["repo", "id"]]
        in_files = file_ids.groupby("repo").size().to_dict()
        con.register("file_ids", file_ids)
        missing = dict(con.execute(f"""
            SELECT repo, count(*) FROM file_ids f ANTI JOIN {table} t USING (repo, id) GROUP BY repo
        """).fetchall())
        con.unregister("file_ids")

        # 2. Raw row count (minus rows staging drops on purpose) == staging row count.
        counts = con.execute(f"""
            SELECT r.repo, r.raw_rows, r.kept_rows, coalesce(s.stg_rows, 0)
            FROM (SELECT repo, count(*) AS raw_rows, count(*) FILTER (WHERE {kept}) AS kept_rows
                  FROM {table} GROUP BY repo) r
            LEFT JOIN (SELECT repo, count(*) AS stg_rows FROM {stg} GROUP BY repo) s USING (repo)
            ORDER BY r.repo
        """).fetchall()

        for repo, raw_rows, kept_rows, stg_rows in counts:
            n_missing = missing.get(repo, 0)
            log.info(f"{repo} | {endpoint}: files={in_files.get(repo, 0)} raw={raw_rows} "
                     f"(kept by staging={kept_rows}) {stg}={stg_rows} missing_from_raw={n_missing}")
            if n_missing:
                failures.append(f"{repo} {endpoint}: {n_missing} records in data/raw/ not in {table}")
            if kept_rows != stg_rows:
                failures.append(f"{repo} {endpoint}: raw keeps {kept_rows} rows but {stg} has {stg_rows}")

    # 3. Commits in every complete month of the window (the run month is exempt).
    empty_months = con.execute("""
        SELECT repo, strftime(month, '%Y-%m') FROM mart_repo_monthly
        WHERE NOT is_partial_month AND commits = 0 ORDER BY repo, month
    """).fetchall()
    n_months = scalar(con, "SELECT count(DISTINCT month) FROM mart_repo_monthly WHERE NOT is_partial_month")
    if empty_months:
        failures.append(f"months with no commits: {empty_months}")
    else:
        log.info(f"Every repo has commits in all {n_months} complete months")

    for f in failures:
        log.error(f"CHECK FAILED: {f}")
    return failures


def find_outliers(con, log: TaskLog) -> dict:
    """
    Flag (but KEEP) extreme values with Tukey fences, computed per repo:
      - Time to merge is heavily right-skewed (minutes to years), so the fence is
        computed on log10(hours): flag if log10(h) > Q3 + 1.5 * IQR.
      - Weekly commits are counts, so the "far out" fence is used on the raw
        values: flag if commits > Q3 + 3 * IQR. Zero-commit weeks are included.
    """
    merge = con.execute("""
        WITH x AS (
            SELECT repo, pr_number, author_login, created_at, hours_to_merge,
                   log10(greatest(hours_to_merge, 1/60)) AS lh   -- floor at 1 minute so log10 is defined
            FROM fct_pull_requests WHERE is_merged
        ),
        fence AS (
            SELECT repo, quantile_cont(lh, 0.75) + 1.5 * (quantile_cont(lh, 0.75) - quantile_cont(lh, 0.25)) AS f
            FROM x GROUP BY repo
        )
        SELECT x.repo, x.pr_number, x.author_login, x.created_at::DATE AS created,
               round(x.hours_to_merge / 24, 1) AS days_to_merge, round(power(10, f) / 24, 1) AS cutoff_days
        FROM x JOIN fence USING (repo)
        WHERE x.lh > fence.f
        ORDER BY x.repo, x.hours_to_merge DESC
    """).df()

    weekly = con.execute(f"""
        WITH weeks AS (    -- complete weeks only, so the partial current week is not a "low" outlier
            SELECT unnest(generate_series(date_trunc('week', TIMESTAMPTZ '{WINDOW_START}'),
                                          date_trunc('week', (SELECT max(fetched_at) FROM stg_commits)) - INTERVAL 7 DAY,
                                          INTERVAL 7 DAY)) AS week
        ),
        counts AS (
            SELECT r.repo, w.week, count(c.sha) AS commits, count(c.sha) FILTER (WHERE c.is_bot) AS bot_commits
            FROM (SELECT DISTINCT repo FROM stg_commits) r CROSS JOIN weeks w
            LEFT JOIN stg_commits c ON c.repo = r.repo AND date_trunc('week', c.committed_at) = w.week
            GROUP BY ALL
        ),
        fence AS (
            SELECT repo, quantile_cont(commits, 0.5) AS median_week,
                   quantile_cont(commits, 0.75) + 3 * (quantile_cont(commits, 0.75) - quantile_cont(commits, 0.25)) AS f
            FROM counts GROUP BY repo
        )
        SELECT c.repo, c.week::DATE AS week, c.commits, c.bot_commits, fence.median_week, round(fence.f, 1) AS cutoff
        FROM counts c JOIN fence USING (repo)
        WHERE c.commits > fence.f
        ORDER BY c.repo, c.commits DESC
    """).df()

    with pd.option_context("display.width", 200, "display.max_columns", 20):
        log.info("Outliers: time to merge (fence on log10 hours: Q3 + 1.5*IQR, per repo). "
                 f"{len(merge)} merged PRs flagged:\n{merge.to_string(index=False)}")
        log.info("Outliers: weekly commits (fence: Q3 + 3*IQR, per repo). "
                 f"{len(weekly)} weeks flagged:\n{weekly.to_string(index=False)}")
    log.info("Treatment: all flagged records are KEPT. They are real events (long-lived PRs, "
             "release/merge bursts), and every scored metric uses medians, shares, or slopes "
             "that a handful of extreme values barely moves.")
    return {"merge_outliers": len(merge), "commit_week_outliers": len(weekly)}


def describe(con, log: TaskLog) -> pd.DataFrame:
    """Per-repo medians and 90th percentiles, read from the marts (nothing recomputed)."""
    stats = con.execute("""
        SELECT repo,
               round(median_hours_to_first_review, 1) AS median_first_review_h,
               round(p90_hours_to_first_review, 1)    AS p90_first_review_h,
               round(median_hours_to_merge, 1)        AS median_merge_h,
               round(p90_hours_to_merge, 1)           AS p90_merge_h,
               round(commits_per_week, 1)             AS commits_per_week,
               round(releases_per_year, 1)            AS releases_per_year
        FROM mart_health_inputs ORDER BY repo
    """).df()
    with pd.option_context("display.width", 200, "display.max_columns", 20):
        log.info(f"Descriptive statistics (hours unless noted):\n{stats.to_string(index=False)}")
    return stats


# ---------- Scoring helpers (Task 5) ----------
# Scored metrics: column in mart_health_inputs -> (dimension, higher is healthier?).
# All are rates, shares, medians, or size-adjusted slopes, so volume alone cannot win.
METRICS = {
    "median_hours_to_first_review": ("responsiveness", False),
    "pct_reviewed_within_7d":       ("responsiveness", True),
    "pct_merged_with_approval":     ("responsiveness", True),
    "merge_rate":                   ("throughput", True),
    "median_hours_to_merge":        ("throughput", False),
    "pct_open_prs_stale":           ("backlog", False),
    "commit_trend":                 ("activity", True),     # trend component
    "pct_merges_by_top5_authors":   ("contributors", False),
    "contributor_trend":            ("contributors", True),  # trend component
    "releases_per_year":            ("releases", True),
    "release_interval_cv":          ("releases", False),
}

# Responsiveness counts most: it is what an outside contributor experiences first.
# Release cadence counts least: projects choose very different release policies on purpose.
DIMENSION_WEIGHTS = {
    "responsiveness": 0.25,
    "throughput":     0.20,
    "backlog":        0.15,
    "activity":       0.15,
    "contributors":   0.15,
    "releases":       0.10,
}


def score(inputs: pd.DataFrame) -> pd.DataFrame:
    """
    Min-max scale each metric across the repos to 0..1 (flipped when lower is
    healthier, so 1 is always best), average metrics within each dimension,
    then take the weighted sum of dimensions. Returns one row per repo, best first.
    """
    scaled = pd.DataFrame(index=inputs.index)
    for metric, (_, higher_is_better) in METRICS.items():
        col = inputs[metric]
        spread = col.max() - col.min()
        s = (col - col.min()) / spread if spread else pd.Series(0.5, index=col.index)  # all equal: neutral
        scaled[metric] = s if higher_is_better else 1 - s

    subscores = pd.DataFrame({
        dim: scaled[[m for m, (d, _) in METRICS.items() if d == dim]].mean(axis=1) * 100
        for dim in DIMENSION_WEIGHTS
    })
    subscores["score"] = sum(subscores[dim] * w for dim, w in DIMENSION_WEIGHTS.items())
    subscores["rank"] = subscores["score"].rank(ascending=False, method="min").astype(int)

    ranking = subscores.reset_index().sort_values("rank")
    return ranking[["rank", "repo", "score", *DIMENSION_WEIGHTS]]


# ---------- Plot helpers (Task 6) ----------
# One fixed color per repo, used in every plot (colorblind-safe categorical slots 1-3).
REPO_COLORS = {
    "numpy/numpy":             "#2a78d6",   # blue
    "networkx/networkx":       "#eb6834",   # orange
    "statsmodels/statsmodels": "#1baf7a",   # aqua
}
plt.rcParams.update({
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": "#e5e5e5", "grid.linewidth": 0.8,
    "axes.titlesize": 11, "axes.labelsize": 9, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "figure.dpi": 150,
})


def rank_label(row) -> str:
    """e.g. '#1 numpy/numpy (64.0)' - used in legends so every plot shows the ranking."""
    return f"#{row['rank']} {row['repo']} ({row['score']:.1f})"


def plot_dimension_profile(ranking: pd.DataFrame) -> Path:
    """Small multiples: one panel per repo (healthiest first), one bar per scored dimension."""
    dims = list(DIMENSION_WEIGHTS)
    labels = [f"{d.capitalize()} ({DIMENSION_WEIGHTS[d]:.0%})" for d in dims]

    fig, axes = plt.subplots(1, len(ranking), figsize=(13, 4.2), sharey=True)
    for ax, (_, row) in zip(axes, ranking.iterrows()):
        values = [row[d] for d in dims]
        ax.barh(labels, values, color=REPO_COLORS[row["repo"]], height=0.6)
        for y, v in enumerate(values):
            ax.text(v + 2, y, f"{v:.0f}", va="center", fontsize=8, color="#333333")
        ax.set_title(rank_label(row), loc="left", fontweight="bold")
        ax.set_xlim(0, 110)
        ax.set_xlabel("Dimension subscore (0 = worst of the three, 100 = best)")
        ax.grid(axis="y", visible=False)
    axes[0].invert_yaxis()   # first dimension (highest weight) on top
    fig.suptitle("Health profile by dimension, ordered from healthiest to least healthy "
                 "(weights in parentheses)", x=0.01, ha="left", fontsize=12)
    fig.tight_layout()
    path = PLOT_DIR / "1_dimension_profile.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_monthly_trends(monthly: pd.DataFrame, ranking: pd.DataFrame) -> Path:
    """Small multiples from mart_repo_monthly: one panel per metric, one line per repo."""
    panels = [
        ("prs_opened",                "PRs opened",                          "PRs / month",  False),
        ("prs_merged",                "PRs merged",                          "PRs / month",  False),
        ("merge_ratio",               "Merge ratio (merged / closed)",       "share",        False),
        ("median_first_review_hours", "Median time to first review",         "hours (log)",  True),
        ("commits",                   "Commits",                             "commits / month", False),
        ("active_contributors",       "Active PR authors (non-bot)",         "authors / month", False),
    ]
    run_month = monthly.loc[monthly["is_partial_month"], "month"].max()

    fig, axes = plt.subplots(2, 3, figsize=(14, 7.5), sharex=True)
    for ax, (col, title, unit, log_y) in zip(axes.flat, panels):
        for _, row in ranking.iterrows():   # legend order = rank order
            d = monthly[monthly["repo"] == row["repo"]]
            ax.plot(d["month"], d[col], color=REPO_COLORS[row["repo"]], linewidth=2, label=rank_label(row))
        ax.axvspan(run_month, run_month + pd.offsets.MonthEnd(1), color="#cccccc", alpha=0.5, lw=0)
        ax.set_title(title, loc="left")
        ax.set_ylabel(unit)
        if log_y:
            ax.set_yscale("log")
    for ax in axes[1]:
        ax.set_xlabel("Month")
        ax.tick_params(axis="x", rotation=30)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", ncol=3, frameon=False, fontsize=9)
    fig.suptitle(f"Monthly activity, Jan 2024 - {run_month:%b %Y} (grey band = partial current month)",
                 x=0.01, ha="left", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    path = PLOT_DIR / "2_monthly_trends.png"
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_review_vs_merge(monthly: pd.DataFrame, ranking: pd.DataFrame) -> Path:
    """
    Bubble chart, one bubble per repo-month (complete months):
    x = median time to first review, y = merge ratio, size = active PR authors, color = repo.
    """
    d = monthly[~monthly["is_partial_month"]].dropna(subset=["median_first_review_hours", "merge_ratio"])

    fig, ax = plt.subplots(figsize=(9, 6))
    for _, row in ranking.iterrows():
        r = d[d["repo"] == row["repo"]]
        ax.scatter(r["median_first_review_hours"], r["merge_ratio"], s=r["active_contributors"] * 6,
                   color=REPO_COLORS[row["repo"]], alpha=0.55, edgecolors="white", linewidths=1.5,
                   label=rank_label(row))
    ax.set_xscale("log")
    ax.set_xlabel("Median time to first review that month (hours, log scale)")
    ax.set_ylabel("Merge ratio that month (merged / (merged + closed unmerged))")
    ax.set_title("Faster review vs. merge ratio, one bubble per repo-month\n"
                 "bubble area = active non-bot PR authors that month", loc="left")
    legend = ax.legend(frameon=False, fontsize=9, loc="lower left")
    for handle in legend.legend_handles:   # same-size, opaque legend dots (size is data, not identity)
        handle.set_alpha(1)
        handle.set_sizes([60])
    fig.tight_layout()
    path = PLOT_DIR / "3_review_vs_merge.png"
    fig.savefig(path)
    plt.close(fig)
    return path


# ---------- Tasks ----------
@task(name="1-extract")
def extract(token: str) -> dict:
    """Task 1: pull commits, PRs, PR reviews, and releases into data/raw/."""
    log = TaskLog("1_extract")
    start_requests, start_time = REQUEST_COUNT, time.time()

    # --- Resume or start fresh? ---
    if LOADED_MARKER.exists():
        log.info("_LOADED found: previous data is safely in DuckDB. Deleting data/raw/ and starting fresh.")
        shutil.rmtree(RAW_DIR)
    elif RAW_DIR.exists():
        log.info("No _LOADED marker: last run did not finish loading. RESUMING; pages on disk will be skipped.")
    else:
        log.info("No data/raw/ yet: first run.")

    watermarks = get_watermarks()
    mode = "incremental" if watermarks else "full"
    log.info(f"Mode: {mode} | repos: {REPOS}")

    counts = {repo: extract_repo(repo, token, watermarks, log) for repo in REPOS}

    used = REQUEST_COUNT - start_requests
    log.info(f"Extract finished ({mode}): {used} API requests in {time.time() - start_time:.0f}s")
    return {"mode": mode, "requests": used, "counts": counts}


@task(name="2-load")
def load(extract_summary: dict) -> dict:
    """
    Task 2: load data/raw/*.json into raw_* tables in DuckDB.
    Everything happens in ONE transaction: either every file loads and
    _load_state + _LOADED are updated, or nothing changes and the next run resumes.
    """
    log = TaskLog("2_load")
    log.info(f"Loading data/raw into {DB_PATH.name}")

    summary = {}
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute(LOAD_STATE_DDL)
        con.begin()
        try:
            for endpoint, (table, id_type, _, _) in RAW_TABLES.items():
                con.execute(raw_table_ddl(table, id_type))
                before = scalar(con, f"SELECT count(*) FROM {table}")

                batch = read_endpoint(endpoint)
                upsert(con, table, batch)

                after = scalar(con, f"SELECT count(*) FROM {table}")
                summary[table] = {"records_in_files": len(batch), "before": before, "after": after}
                # Idempotency evidence: loading the same files twice gives before == after.
                log.info(f"{table}: {len(batch)} distinct records in files | "
                         f"rows before={before}, after={after} (+{after - before})")

            # Only after EVERY file loaded: record the newest updated_at per repo/endpoint.
            loaded_at = utc_now_str()
            for endpoint, (table, _, _, _) in RAW_TABLES.items():
                con.execute(f"""
                    INSERT OR REPLACE INTO _load_state
                    SELECT repo, ?, max(updated_at), ?::TIMESTAMP FROM {table} GROUP BY repo
                """, [endpoint, loaded_at])
            con.commit()
        except Exception:
            con.rollback()
            log.error("Load failed; rolled back. No _LOADED marker written, so the next run resumes.")
            raise

        for repo, endpoint, max_updated in con.execute(
                "SELECT repo, endpoint, max_updated_at FROM _load_state ORDER BY repo, endpoint").fetchall():
            log.info(f"_load_state | {repo} | {endpoint}: watermark {max_updated}")

    LOADED_MARKER.touch()  # tells Task 1 next time: everything on disk is safe in DuckDB
    log.info(f"Load complete; wrote {LOADED_MARKER.relative_to(PROJECT_DIR)}")
    return summary


@task(name="3-dbt-build")
def dbt_build(load_summary: dict) -> dict:
    """
    Task 3: run `dbt build` (every model + every test) and fail the flow on any
    model or test failure. This is the only place dbt runs.
    """
    log = TaskLog("3_dbt")

    # Absolute paths, so dbt works no matter which folder the flow is started from.
    os.environ["DBT_DUCKDB_PATH"] = str(DB_PATH)   # read by profiles.yml
    os.environ["DBT_LOG_PATH"] = str(LOG_DIR)      # dbt's own log -> logs/dbt.log
    log.info(f"Running dbt build against {DB_PATH.name}")

    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir=str(PROJECT_DIR), profiles_dir=str(PROJECT_DIR)),
        raise_on_failure=False,   # we log every result ourselves first, then raise below
    )
    result = runner.invoke(["build"])

    if result.result is None:   # dbt could not even start (bad YAML, missing profile...)
        raise RuntimeError(f"dbt build crashed before running: {result.exception}")

    # One line per model/test, e.g. "test | unique_stg_commits_sha | pass"
    statuses = {}
    for r in result.result.results:
        status = str(r.status)
        statuses[status] = statuses.get(status, 0) + 1
        line = f"{r.node.resource_type} | {r.node.name} | {status}"
        if status in ("pass", "success"):
            log.info(line)
        else:
            log.error(f"{line} | {r.message}")
    log.info(f"dbt build summary: {statuses}")

    if not result.success:
        raise RuntimeError(f"dbt build failed: {statuses} (details in logs/3_dbt.log and logs/dbt.log)")
    return statuses


@task(name="4-validate")
def validate(dbt_summary: dict) -> dict:
    """
    Task 4 (Python, no dbt): check nothing was lost from files -> raw -> staging,
    flag outliers, and print descriptive statistics. Any failed check raises.
    """
    log = TaskLog("4_validate")
    with connect_db() as con:
        failures = check_completeness(con, log)
        outliers = find_outliers(con, log)
        stats = describe(con, log)

    if failures:
        raise ValueError(f"{len(failures)} completeness check(s) failed: " + "; ".join(failures))
    log.info("All completeness checks passed")
    return {"outliers": outliers, "stats": stats.to_dict("records")}


@task(name="5-model")
def model(validation: dict) -> pd.DataFrame:
    """
    Task 5: score every metric in mart_health_inputs, average them into dimension
    subscores, combine those with DIMENSION_WEIGHTS, and rank the repos.
    """
    log = TaskLog("5_model")
    with connect_db() as con:
        inputs = con.execute("SELECT * FROM mart_health_inputs").df().set_index("repo")

    for metric, (dimension, higher_is_better) in METRICS.items():
        log.info(f"Metric {metric} [{dimension}] ({'higher' if higher_is_better else 'lower'} is healthier): "
                 + ", ".join(f"{repo}={value:.3f}" for repo, value in inputs[metric].items()))

    ranking = score(inputs)

    with pd.option_context("display.width", 200, "display.max_columns", 20):
        log.info(f"Health ranking (subscores 0-100; weights {DIMENSION_WEIGHTS}):\n"
                 f"{ranking.round(1).to_string(index=False)}")

    # Write back to the database so the result lives next to the marts.
    with connect_db() as con:
        con.register("ranking_df", ranking)
        con.execute("CREATE OR REPLACE TABLE health_ranking AS SELECT * FROM ranking_df")
    log.info("Wrote table health_ranking to packages.duckdb")
    return ranking


@task(name="6-plot")
def plot(ranking: pd.DataFrame) -> list:
    """Task 6: save three multi-dimensional PNG plots to plots/."""
    log = TaskLog("6_plot")
    PLOT_DIR.mkdir(exist_ok=True)

    with connect_db() as con:
        monthly = con.execute("SELECT * FROM mart_repo_monthly ORDER BY repo, month").df()

    paths = [
        plot_dimension_profile(ranking),
        plot_monthly_trends(monthly, ranking),
        plot_review_vs_merge(monthly, ranking),
    ]
    for p in paths:
        log.info(f"Saved {p.relative_to(PROJECT_DIR).as_posix()}")
    return paths


# ---------- Flow ----------
@flow(name="xad2vu-package-health-flow")  # named with my computing ID so staff can find my runs
def package_health_flow():
    """The single entry point: runs all six tasks in order."""

    # Token comes from an environment variable, never a file in the repo.
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is not set")

    start = time.time()

    # Each task's return value feeds the next, which makes the order explicit.
    extract_summary = extract(token)
    load_summary = load(extract_summary)
    dbt_summary = dbt_build(load_summary)
    validation = validate(dbt_summary)
    ranking = model(validation)
    plot(ranking)

    # API odometer: every GitHub request in this run goes through fetch_page.
    get_run_logger().info(f"API odometer: {REQUEST_COUNT} GitHub requests this run "
                          f"({extract_summary['mode']} extract); total runtime {time.time() - start:.0f}s")


# Only run the flow when this file is executed directly (python main.py).
if __name__ == "__main__":
    package_health_flow()