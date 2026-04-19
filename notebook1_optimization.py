# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 DataPulse — Optimization Analysis Summary
# MAGIC
# MAGIC **Purpose:** Nightly scan of all Databricks tables, views, and jobs to surface:
# MAGIC - Small file issues with OPTIMIZE recommendations
# MAGIC - Missing liquid clustering opportunities
# MAGIC - Long-running and frequently failing jobs
# MAGIC - Stale assets (tables, views, jobs)
# MAGIC - Estimated monthly compute cost and potential savings
# MAGIC
# MAGIC **Schedule:** Daily at 11 PM ET | **Output:** Professional HTML email report

# COMMAND ----------
# MAGIC %md ## CELL 2 — Configuration and Parameters

# COMMAND ----------

import json
import html
import traceback
import requests
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytz
from pyspark.sql import functions as F, Row
from pyspark.sql.types import StringType

# ── Widgets ────────────────────────────────────────────────────────────────────
dbutils.widgets.text("email_to", "your_email@domain.com", "Email To")
dbutils.widgets.text("email_cc", "",                       "Email CC")

EMAIL_TO  = dbutils.widgets.get("email_to").strip()
EMAIL_CC  = dbutils.widgets.get("email_cc").strip()

# ── Constants ──────────────────────────────────────────────────────────────────
CATALOG       = "source_catalog"
SCHEMAS       = ["raw_schema", "transform_schema"]
DEST          = "target_schema"
TRACK_TBL     = f"{DEST}.optimization_run_tracker"
EMAIL_FROM    = "optimization_from_address@domain.com"
EMAIL_NB      = "/Repos/repo_path/email_notification_notebook"
EXCLUDED_JOBS = {"exclude_jobs_list"}
STALE_DAYS    = 60
TZ            = pytz.timezone("America/New_York")

# ── API Setup ──────────────────────────────────────────────────────────────────
WORKSPACE_HOST = spark.conf.get("spark.databricks.workspaceUrl")
API_BASE       = f"https://{WORKSPACE_HOST}"
TOKEN          = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext().apiToken().get()
)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# ── Timestamps ─────────────────────────────────────────────────────────────────
now_et            = datetime.now(TZ)
today_str         = now_et.strftime("%Y-%m-%d")
stale_cutoff_dt   = now_et - timedelta(days=STALE_DAYS)
stale_cutoff_str  = stale_cutoff_dt.strftime("%Y-%m-%d")
stale_cutoff_ms   = int(stale_cutoff_dt.timestamp() * 1000)

print(f"[DataPulse-NB1] Run date  : {today_str}")
print(f"[DataPulse-NB1] Stale cutoff: {stale_cutoff_str}")

# ── Tracking Table ─────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TRACK_TBL} (
        run_date      DATE,
        analysis_json STRING,
        created_at    TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# ── Gap Handling ───────────────────────────────────────────────────────────────
row = spark.sql(f"SELECT MAX(run_date) AS last_run FROM {TRACK_TBL}").first()
lookback_start_str = str(row["last_run"]) if row["last_run"] else today_str
lookback_start_dt  = datetime.strptime(lookback_start_str, "%Y-%m-%d").replace(tzinfo=TZ)
lookback_start_ms  = int(lookback_start_dt.timestamp() * 1000)

print(f"[DataPulse-NB1] Lookback start: {lookback_start_str}")

# COMMAND ----------
# MAGIC %md ## CELL 3 — Collect Table and View Metadata

# COMMAND ----------

def _api_get(path: str, params: dict = None) -> dict:
    """Thin wrapper around Databricks REST GET with error handling."""
    resp = requests.get(f"{API_BASE}{path}", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def collect_table_metadata(catalog: str, schemas: list[str]) -> tuple[list[dict], list[dict]]:
    """
    Returns (tables, views) as lists of dicts.
    Uses DESCRIBE DETAIL per table — parallelised for speed.
    Excludes __materialization and event_log prefixes.
    """
    tables, views = [], []
    EXCLUDE_PREFIXES = ("__materialization", "event_log")
    TABLE_TYPES      = ("MANAGED", "EXTERNAL")
    VIEW_TYPES       = ("VIEW", "MATERIALIZED_VIEW")

    for schema in schemas:
        rows = spark.sql(f"""
            SELECT table_name, table_type, created, last_altered
            FROM   {catalog}.information_schema.tables
            WHERE  table_schema = '{schema}'
        """).collect()

        tbl_rows  = [r for r in rows if r.table_type in TABLE_TYPES
                     and not r.table_name.startswith(EXCLUDE_PREFIXES)]
        view_rows = [r for r in rows if r.table_type in VIEW_TYPES
                     and not r.table_name.startswith(EXCLUDE_PREFIXES)]

        # ── Views (lightweight) ────────────────────────────────────────────────
        for r in view_rows:
            views.append({
                "schema":  schema,
                "name":    r.table_name,
                "ttype":   r.table_type,
                "created": str(r.created),
                "altered": str(r.last_altered),
            })

        # ── Tables: parallelise DESCRIBE DETAIL ───────────────────────────────
        def _describe(schema_name: str, tbl_name: str, created, altered) -> dict | None:
            try:
                fqn  = f"{catalog}.{schema_name}.{tbl_name}"
                det  = spark.sql(f"DESCRIBE DETAIL {fqn}").first()
                sz   = (det["sizeInBytes"] or 0) / (1024 ** 2)
                files = det["numFiles"] or 0
                return {
                    "schema":     schema_name,
                    "name":       tbl_name,
                    "ttype":      "TABLE",
                    "fmt":        det["format"],
                    "created":    str(created),
                    "altered":    str(altered),
                    "size_mb":    round(sz, 2),
                    "files":      files,
                    "avg_file_mb": round(sz / files, 3) if files > 0 else 0,
                    "part_cols":  str(det["partitionColumns"]),
                    "clust_cols": str(det["clusteringColumns"]),
                }
            except Exception as ex:
                print(f"  [WARN] DESCRIBE DETAIL failed for {tbl_name}: {ex}")
                return None

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = {
                pool.submit(_describe, schema, r.table_name, r.created, r.last_altered): r
                for r in tbl_rows
            }
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    tables.append(result)

    return tables, views


tables, views = collect_table_metadata(CATALOG, SCHEMAS)

total_gb = sum(t["size_mb"] for t in tables) / 1024
print(f"[DataPulse-NB1] Tables : {len(tables)}  ({total_gb:.2f} GB)")
print(f"[DataPulse-NB1] Views  : {len(views)}")

# COMMAND ----------
# MAGIC %md ## CELL 4 — Collect Job Data and Failures

# COMMAND ----------

def paginate_jobs() -> list[dict]:
    """Return all jobs, excluding EXCLUDED_JOBS, via paginated API."""
    jobs, offset = [], 0
    while True:
        data = _api_get("/api/2.1/jobs/list", {"limit": 25, "offset": offset})
        batch = [j for j in data.get("jobs", [])
                 if j.get("settings", {}).get("name", "") not in EXCLUDED_JOBS]
        jobs.extend(batch)
        if not data.get("has_more"):
            break
        offset += 25
    return jobs


def analyse_job(job: dict) -> tuple[dict, list[dict]]:
    """
    Fetch last 25 runs for a job.
    Returns (job_summary_dict, list_of_failure_dicts).
    """
    jid      = job["job_id"]
    jname    = job.get("settings", {}).get("name", f"Job #{jid}")
    schedule = job.get("settings", {}).get("schedule", {}).get("quartz_cron_expression", "")

    try:
        data = _api_get("/api/2.1/jobs/runs/list", {"job_id": jid, "limit": 25})
        runs = data.get("runs", [])
    except Exception:
        runs = []

    durations, failures, last_ts = [], [], 0

    for run in runs:
        start = run.get("start_time", 0)
        end   = run.get("end_time", 0) or (start + 1)
        dur_m = (end - start) / 60_000

        if start > last_ts:
            last_ts = start

        if run.get("state", {}).get("result_state") == "FAILED":
            failures.append(run)
            if start >= lookback_start_ms:
                err = (run.get("state", {}).get("state_message") or "")[:500]
                failures_since_lookback.append({
                    "job_name": jname,
                    "job_id":   jid,
                    "run_id":   run.get("run_id"),
                    "error":    err,
                    "url":      run.get("run_page_url", ""),
                    "duration": round(dur_m, 1),
                    "date":     datetime.fromtimestamp(start / 1000, TZ).strftime("%Y-%m-%d %H:%M ET"),
                })
        if durations is not None:
            durations.append(dur_m)

    avg_m     = round(sum(durations) / len(durations), 1) if durations else 0
    max_m     = round(max(durations), 1)                  if durations else 0
    fail_rate = round(len(failures) / len(runs) * 100, 1) if runs      else 0
    last_run  = (datetime.fromtimestamp(last_ts / 1000, TZ).strftime("%Y-%m-%d %H:%M ET")
                 if last_ts else "Never")

    summary = {
        "job_id":    jid,
        "job_name":  jname,
        "schedule":  schedule,
        "avg_min":   avg_m,
        "max_min":   max_m,
        "fail_rate": fail_rate,
        "last_run":  last_run,
        "last_ts":   last_ts,
    }
    return summary


# Shared list populated inside analyse_job
failures_since_lookback: list[dict] = []

raw_jobs = paginate_jobs()
print(f"[DataPulse-NB1] Jobs found: {len(raw_jobs)}")

job_data: list[dict] = []
with ThreadPoolExecutor(max_workers=20) as pool:
    for summary in pool.map(analyse_job, raw_jobs):
        job_data.append(summary)

period_failures = failures_since_lookback.copy()
print(f"[DataPulse-NB1] Period failures: {len(period_failures)}")

# COMMAND ----------
# MAGIC %md ## CELL 5 — Identify Optimization Issues

# COMMAND ----------

# ── 1. Small Files ─────────────────────────────────────────────────────────────
def classify_small_file_severity(files: int, avg_mb: float) -> str:
    if files > 1000 or avg_mb < 1:
        return "CRITICAL"
    if files > 200:
        return "HIGH"
    return "MEDIUM"


small_files = []
for t in tables:
    files, avg_mb, size_mb = t["files"], t["avg_file_mb"], t["size_mb"]
    is_small = (files > 50 and avg_mb < 32) or files > 200
    if is_small:
        sev       = classify_small_file_severity(files, avg_mb)
        # Estimate scan improvement: fewer, larger files → better scan efficiency
        gain_pct  = min(round((1 - min(avg_mb, 128) / 128) * 100), 80)
        fqn       = f"{CATALOG}.{t['schema']}.{t['name']}"
        small_files.append({
            **t,
            "fqn":      fqn,
            "severity": sev,
            "gain_pct": gain_pct,
            "cmd":      f"OPTIMIZE {fqn};",
        })

small_files.sort(key=lambda x: (-{"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1}[x["severity"]], -x["files"]))
print(f"[DataPulse-NB1] Small file tables: {len(small_files)}")


# ── 2. Missing Clustering ──────────────────────────────────────────────────────
DATE_PATTERNS = [
    "summary_date", "history_date", "calendar_date", "created_date",
    "conversation_start", "_date", "_time", "_tmst", "created", "_day",
]
ID_PATTERNS = [
    "customer_number", "client_iid", "client_id", "cust_", "plan_number",
    "rs_plan_number", "_id", "_key", "_number", "_iid", "account",
]


def recommend_cluster_cols(schema: str, name: str) -> list[str]:
    """Batch-query columns for a single table and score for clustering."""
    cols_df = spark.sql(f"""
        SELECT column_name
        FROM   {CATALOG}.information_schema.columns
        WHERE  table_schema = '{schema}' AND table_name = '{name}'
    """)
    col_names = [r.column_name.lower() for r in cols_df.collect()]

    date_cols = [c for c in col_names if any(p in c for p in DATE_PATTERNS)]
    id_cols   = [c for c in col_names if any(p in c for p in ID_PATTERNS) and c not in date_cols]

    chosen = []
    if date_cols:
        chosen.append(date_cols[0])
        chosen.extend(id_cols[:2])
    else:
        chosen.extend(id_cols[:3])

    return chosen[:3]


no_cluster_candidates = [
    t for t in tables
    if t["clust_cols"] in ("[]", "None", "") and t["size_mb"] > 100
]

missing_cluster: list[dict] = []

def _enrich_cluster(t: dict) -> dict:
    rec_cols = recommend_cluster_cols(t["schema"], t["name"])
    fqn      = f"{CATALOG}.{t['schema']}.{t['name']}"
    cols_str  = ", ".join(rec_cols) if rec_cols else "-- no suitable columns found"
    alter_cmd = f"ALTER TABLE {fqn} CLUSTER BY ({cols_str});" if rec_cols else ""
    opt_cmd   = f"OPTIMIZE {fqn};"
    return {**t, "fqn": fqn, "rec_cols": rec_cols, "alter_cmd": alter_cmd, "opt_cmd": opt_cmd}

with ThreadPoolExecutor(max_workers=12) as pool:
    missing_cluster = list(pool.map(_enrich_cluster, no_cluster_candidates))

missing_cluster.sort(key=lambda x: -x["size_mb"])
print(f"[DataPulse-NB1] Missing clustering tables: {len(missing_cluster)}")


# ── 3. Long-running Jobs ───────────────────────────────────────────────────────
long_running = sorted(
    [j for j in job_data if j["avg_min"] > 10],
    key=lambda x: -x["avg_min"],
)
print(f"[DataPulse-NB1] Long-running jobs: {len(long_running)}")


# ── 4. Frequently Failing Jobs ─────────────────────────────────────────────────
failing_jobs = sorted(
    [j for j in job_data if j["fail_rate"] > 20],
    key=lambda x: -x["fail_rate"],
)
print(f"[DataPulse-NB1] Frequently failing jobs: {len(failing_jobs)}")


# ── 5. Stale Assets ────────────────────────────────────────────────────────────
stale_tables = [t for t in tables if t.get("altered", "") < stale_cutoff_str]
stale_views  = [v for v in views   if v.get("altered", "") < stale_cutoff_str]
stale_jobs   = [j for j in job_data
                if j["last_ts"] < stale_cutoff_ms or j["last_run"] == "Never"]
print(f"[DataPulse-NB1] Stale — tables:{len(stale_tables)} views:{len(stale_views)} jobs:{len(stale_jobs)}")


# ── 6. Cost Estimates ──────────────────────────────────────────────────────────
total_daily_minutes     = sum(j["avg_min"] for j in job_data)
optimizable_minutes     = sum(j["avg_min"] for j in long_running)
est_monthly             = round(total_daily_minutes     * 30 * 0.07 / 60, 2)
est_savings             = round(optimizable_minutes     * 0.3 * 30 * 0.07 / 60, 2)
print(f"[DataPulse-NB1] Est. monthly: ${est_monthly}  Potential savings: ${est_savings}")


# ── Cron-to-Human ──────────────────────────────────────────────────────────────
def cron_to_human(expr: str) -> str:
    """Convert Quartz cron expression to a readable string."""
    if not expr:
        return "No schedule"
    DAY_MAP = {
        "1": "Sun", "2": "Mon", "3": "Tue", "4": "Wed",
        "5": "Thu", "6": "Fri", "7": "Sat",
        "MON": "Mon", "TUE": "Tue", "WED": "Wed", "THU": "Thu",
        "FRI": "Fri", "SAT": "Sat", "SUN": "Sun",
    }
    try:
        parts = expr.strip().split()
        if len(parts) < 6:
            return expr
        _, minutes, hours, dom, _, dow = parts[:6]

        # Hour / minute
        if hours.isdigit() and minutes.isdigit():
            h, m = int(hours), int(minutes)
            suffix = "AM" if h < 12 else "PM"
            h12    = h % 12 or 12
            time_s = f"{h12}:{m:02d} {suffix}"
        else:
            time_s = f"{hours}:{minutes}"

        # Day-of-week
        if dow in ("*", "?"):
            if dom in ("*", "?"):
                return f"Daily at {time_s}"
            return f"Day {dom} of month at {time_s}"

        if "-" in dow:
            lo, hi = dow.split("-")
            return f"{DAY_MAP.get(lo, lo)}-{DAY_MAP.get(hi, hi)} at {time_s}"

        days = [DAY_MAP.get(d, d) for d in dow.split(",")]
        return f"{', '.join(days)} at {time_s}"
    except Exception:
        return expr

# COMMAND ----------
# MAGIC %md ## CELL 6 — Generate Professional HTML Email

# COMMAND ----------

# ── HTML Helpers ───────────────────────────────────────────────────────────────
BADGE_COLORS = {
    "CRITICAL": "#b71c1c",
    "HIGH":     "#d84315",
    "MEDIUM":   "#ef6c00",
    "LOW":      "#2e7d32",
    "GOOD":     "#2e7d32",
}

def badge(text: str, bg: str = "#607d8b") -> str:
    return (
        f'<span style="background:{bg};color:#fff;padding:2px 8px;'
        f'border-radius:12px;font-size:11px;font-weight:600;'
        f'white-space:nowrap">{html.escape(str(text))}</span>'
    )

def severity_badge(sev: str) -> str:
    return badge(sev, BADGE_COLORS.get(sev, "#607d8b"))

def html_tbl(headers: list[str], rows: list[list]) -> str:
    """Styled HTML table with alternating rows."""
    th_style = (
        'style="background:#eef1f5;color:#1a3c5e;padding:8px 12px;'
        'text-align:left;font-size:12px;font-weight:700;'
        'border-bottom:2px solid #d0d7de"'
    )
    thead = "".join(f"<th {th_style}>{html.escape(str(h))}</th>" for h in headers)
    tbody = ""
    for i, row in enumerate(rows):
        bg  = "#ffffff" if i % 2 == 0 else "#f6f8fa"
        tds = "".join(
            f'<td style="padding:7px 12px;font-size:12px;'
            f'border-bottom:1px solid #d0d7de;background:{bg}">{cell}</td>'
            for cell in row
        )
        tbody += f"<tr>{tds}</tr>"
    return (
        f'<table style="width:100%;border-collapse:collapse;'
        f'border:1px solid #d0d7de;margin-bottom:16px">'
        f"<thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>"
    )

def code_blk(code: str) -> str:
    return (
        f'<pre style="background:#1e293b;color:#94d2bd;padding:12px 16px;'
        f'border-radius:6px;font-size:12px;overflow-x:auto;margin:8px 0">'
        f"{html.escape(code)}</pre>"
    )

def section_header(title: str, icon: str = "") -> str:
    return (
        f'<h2 style="color:#1a3c5e;font-size:16px;font-weight:700;'
        f'border-bottom:2px solid #1a3c5e;padding-bottom:6px;margin-top:28px">'
        f"{icon} {html.escape(title)}</h2>"
    )

def dashboard_card(label: str, value: Any, bg: str, unit: str = "") -> str:
    return (
        f'<td style="background:{bg};padding:16px 20px;text-align:center;'
        f'border-radius:8px;min-width:110px">'
        f'<div style="font-size:28px;font-weight:800;color:#1a3c5e">{value}{unit}</div>'
        f'<div style="font-size:11px;color:#37474f;margin-top:4px">{html.escape(label)}</div>'
        f"</td>"
    )


# ── Build Email ────────────────────────────────────────────────────────────────
def build_email() -> str:
    parts = []

    # ── Header banner ─────────────────────────────────────────────────────────
    parts.append(f"""
    <div style="background:linear-gradient(135deg,#1a3c5e,#2c5f8a);
                padding:28px 32px;border-radius:10px 10px 0 0">
      <div style="font-size:24px;font-weight:800;color:#ffffff">
        🔍 DataPulse — Optimization Analysis
      </div>
      <div style="color:#c8dce8;font-size:13px;margin-top:6px">
        {today_str} &nbsp;|&nbsp; Lookback from: {lookback_start_str}
      </div>
    </div>
    """)

    # ── Dashboard cards ────────────────────────────────────────────────────────
    parts.append(f"""
    <table style="width:100%;border-spacing:8px;margin:16px 0"><tr>
      {dashboard_card("Small File Issues",   len(small_files),    "#fff8e1")}
      {dashboard_card("Missing Clustering",  len(missing_cluster),"#e8eaf6")}
      {dashboard_card("Failing Jobs",        len(failing_jobs),   "#ffebee")}
      {dashboard_card("Period Failures",     len(period_failures),"#e0f2f1")}
      {dashboard_card("Est. Monthly Savings","${est_savings}",    "#e8f5e9")}
    </tr></table>
    """)

    # ── Small Files ────────────────────────────────────────────────────────────
    parts.append(section_header("Small File Issues", "⚡"))
    if small_files:
        rows = [
            [
                f'<code style="font-size:11px">{r["schema"]}.{r["name"]}</code>',
                f'{r["size_mb"]:,.0f}',
                f'{r["files"]:,}',
                f'{r["avg_file_mb"]:.2f}',
                severity_badge(r["severity"]),
                f'~{r["gain_pct"]}%',
            ]
            for r in small_files[:15]
        ]
        parts.append(html_tbl(["Table", "Size MB", "Files", "Avg File MB", "Severity", "Expected Gain"], rows))
        parts.append("<p style='font-size:12px;font-weight:700'>OPTIMIZE Commands:</p>")
        parts.append(code_blk("\n".join(r["cmd"] for r in small_files[:10])))
    else:
        parts.append("<p style='color:#2e7d32'>✅ No small file issues detected.</p>")

    # ── Missing Clustering ─────────────────────────────────────────────────────
    parts.append(section_header("Missing Liquid Clustering", "🔗"))
    parts.append("""<p style='font-size:12px;color:#37474f'>
        Liquid clustering replaces traditional partitioning and file ordering.
        Expected performance improvement: <strong>40–70%</strong> on selective queries.
    </p>""")
    if missing_cluster:
        rows = [
            [
                f'<code style="font-size:11px">{r["schema"]}.{r["name"]}</code>',
                f'{r["size_mb"]:,.0f}',
                f'{r["files"]:,}',
                f'<code style="font-size:11px">{", ".join(r["rec_cols"]) or "—"}</code>',
                r["part_cols"],
            ]
            for r in missing_cluster[:15]
        ]
        parts.append(html_tbl(["Table", "Size MB", "Files", "Recommended Cols", "Current Partitions"], rows))
        cmds = "\n".join(
            f'{r["alter_cmd"]}\n{r["opt_cmd"]}' for r in missing_cluster[:8] if r["alter_cmd"]
        )
        parts.append("<p style='font-size:12px;font-weight:700'>ALTER + OPTIMIZE Commands:</p>")
        parts.append(code_blk(cmds))
    else:
        parts.append("<p style='color:#2e7d32'>✅ All large tables have clustering configured.</p>")

    # ── Long-running Jobs ──────────────────────────────────────────────────────
    parts.append(section_header("Long-Running Jobs", "⏱️"))
    parts.append(f"""<p style='font-size:12px;color:#37474f'>
        Estimated monthly compute cost: <strong>${est_monthly}</strong> &nbsp;|&nbsp;
        Potential savings with optimisation: <strong>${est_savings}</strong>
    </p>""")
    if long_running:
        rows = [
            [
                html.escape(j["job_name"]),
                j["avg_min"],
                j["max_min"],
                html.escape(cron_to_human(j["schedule"])),
                f'{j["fail_rate"]}%',
            ]
            for j in long_running
        ]
        parts.append(html_tbl(["Job", "Avg min", "Max min", "Schedule", "Fail Rate"], rows))
        parts.append("""<ol style='font-size:12px;color:#37474f;line-height:1.8'>
            <li>Review query plans with EXPLAIN EXTENDED</li>
            <li>Run OPTIMIZE on upstream source tables</li>
            <li>Add liquid clustering on high-cardinality join keys</li>
            <li>Tune cluster size and auto-scaling thresholds</li>
            <li>Cache intermediate DataFrames where reused</li>
            <li>Reduce shuffle volume — filter before join</li>
        </ol>""")
        for j in long_running[:3]:
            fqn_hint = f"{CATALOG}.<schema>.<table>"
            parts.append(f"<p style='font-size:12px;font-weight:700'>🔎 {html.escape(j['job_name'])}</p>")
            parts.append(code_blk(
                f"-- Diagnostic template for: {j['job_name']}\n"
                f"EXPLAIN EXTENDED SELECT * FROM {fqn_hint} WHERE <predicate>;\n"
                f"-- Job URL: {API_BASE}/#job/{j['job_id']}/runs"
            ))
    else:
        parts.append("<p style='color:#2e7d32'>✅ No long-running jobs detected.</p>")

    # ── Frequently Failing Jobs ────────────────────────────────────────────────
    parts.append(section_header("Frequently Failing Jobs", "❌"))
    if failing_jobs:
        rows = [
            [
                html.escape(j["job_name"]),
                f'{j["fail_rate"]}%',
                "—",
                j["avg_min"],
                html.escape(cron_to_human(j["schedule"])),
                html.escape(j["last_run"]),
            ]
            for j in failing_jobs
        ]
        parts.append(html_tbl(["Job", "Fail Rate", "Failed/Total", "Avg min", "Schedule", "Last Run"], rows))
        parts.append("""<ol style='font-size:12px;color:#37474f;line-height:1.8'>
            <li>Review driver logs for root-cause error patterns</li>
            <li>Verify upstream data pipelines and source availability</li>
            <li>Check cluster libraries and runtime version compatibility</li>
            <li>Inspect for schema drift on source tables</li>
            <li>Add retry policies (maxRetries: 2, retryOnTimeout: true)</li>
            <li>Configure alerting on consecutive failures</li>
        </ol>""")
        for j in failing_jobs[:3]:
            parts.append(f"<p style='font-size:12px;font-weight:700'>🔎 {html.escape(j['job_name'])}</p>")
            parts.append(code_blk(
                f"-- Lookup tables touched by: {j['job_name']}\n"
                f"SHOW TABLES IN {CATALOG}.<schema>;\n\n"
                f"-- Add retry policy snippet (job settings JSON):\n"
                f'-- "max_retries": 2, "retry_on_timeout": true'
            ))
    else:
        parts.append("<p style='color:#2e7d32'>✅ No frequently failing jobs detected.</p>")

    # ── Period Failures ────────────────────────────────────────────────────────
    parts.append(section_header("Period Failures", "🚨"))
    if period_failures:
        for f in period_failures[:20]:
            err_safe = html.escape((f.get("error") or "")[:300])
            parts.append(f"""
            <div style="border-left:4px solid #b71c1c;padding:12px 16px;
                        margin:8px 0;background:#fff5f5;border-radius:4px">
              <strong style="font-size:13px">{html.escape(f['job_name'])}</strong>
              <span style="font-size:11px;color:#666;margin-left:12px">{f['date']}</span>
              &nbsp;|&nbsp;
              <span style="font-size:11px">⏱ {f['duration']} min</span>
              &nbsp;|&nbsp;
              <a href="{f['url']}" style="font-size:11px">Run #{f['run_id']}</a>
              <pre style="margin:8px 0 0;font-size:11px;color:#b71c1c;
                          white-space:pre-wrap">{err_safe}</pre>
            </div>
            """)
        parts.append("""<ol style='font-size:12px;color:#37474f;line-height:1.8;margin-top:12px'>
            <li>Check cluster event log for OOM or spot-instance termination</li>
            <li>Confirm source table schemas haven't changed unexpectedly</li>
            <li>Validate network connectivity to external data sources</li>
            <li>Review concurrent job scheduling for resource contention</li>
            <li>Enable Databricks Enhanced Monitoring for proactive alerts</li>
        </ol>""")
    else:
        parts.append(f"<p style='color:#2e7d32'>✅ No failures since {lookback_start_str}.</p>")

    # ── Stale Assets ───────────────────────────────────────────────────────────
    parts.append(section_header("Stale Assets", "🕐"))
    if stale_tables:
        rows = [[f'<code style="font-size:11px">{t["schema"]}.{t["name"]}</code>',
                 f'{t["size_mb"]:,.0f}', t["altered"]] for t in stale_tables[:15]]
        parts.append("<p style='font-size:12px;font-weight:600'>Tables</p>")
        parts.append(html_tbl(["Table", "Size MB", "Last Altered"], rows))
    if stale_views:
        rows = [[f'<code style="font-size:11px">{v["schema"]}.{v["name"]}</code>',
                 v["altered"]] for v in stale_views[:15]]
        parts.append("<p style='font-size:12px;font-weight:600'>Views</p>")
        parts.append(html_tbl(["View", "Last Altered"], rows))
    if stale_jobs:
        rows = [[html.escape(j["job_name"]),
                 html.escape(cron_to_human(j["schedule"])), j["last_run"]] for j in stale_jobs[:15]]
        parts.append("<p style='font-size:12px;font-weight:600'>Jobs</p>")
        parts.append(html_tbl(["Job", "Schedule", "Last Run"], rows))
    if not any([stale_tables, stale_views, stale_jobs]):
        parts.append("<p style='color:#2e7d32'>✅ No stale assets detected.</p>")

    # ── Recommended Actions ────────────────────────────────────────────────────
    parts.append(section_header("Recommended Actions", "📋"))
    actions = [
        ("#b71c1c", f"Run OPTIMIZE on {len(small_files)} small-file tables immediately"),
        ("#b71c1c", f"Investigate and fix {len(failing_jobs)} frequently failing jobs"),
        ("#d84315", f"Add liquid clustering to {len(missing_cluster)} large unclustered tables"),
        ("#d84315", f"Tune {len(long_running)} long-running jobs (review plans, add clustering)"),
        ("#ef6c00", "Enable predictive optimization in Unity Catalog for auto-OPTIMIZE"),
        ("#ef6c00", f"Clean up or archive {len(stale_tables) + len(stale_views)} stale assets"),
        ("#2e7d32", "Stagger job schedules to avoid cluster resource contention"),
        ("#2e7d32", "Schedule weekly VACUUM RETAIN 168 HOURS on high-churn Delta tables"),
    ]
    alist = "".join(
        f'<li style="margin:6px 0">'
        f'{badge(f"{i+1}", c)} '
        f'<span style="font-size:13px">{html.escape(a)}</span></li>'
        for i, (c, a) in enumerate(actions)
    )
    parts.append(f'<ol style="list-style:none;padding:0">{alist}</ol>')

    # ── Footer ─────────────────────────────────────────────────────────────────
    parts.append(f"""
    <div style="background:#eef1f5;padding:12px 20px;border-radius:0 0 10px 10px;
                margin-top:24px;font-size:11px;color:#555;text-align:center">
      Generated by <strong>DataPulse</strong> on {now_et.strftime('%Y-%m-%d %H:%M ET')}
      &nbsp;|&nbsp; Tables: {len(tables)} &nbsp;|&nbsp; Views: {len(views)}
      &nbsp;|&nbsp; Jobs: {len(job_data)} &nbsp;|&nbsp; Issues: {len(small_files)+len(missing_cluster)+len(failing_jobs)}
    </div>
    """)

    wrapper = (
        '<div style="font-family:Segoe UI,Arial,sans-serif;max-width:960px;'
        'margin:0 auto;padding:20px">'
        + "".join(parts)
        + "</div>"
    )
    return wrapper


email_body = build_email()
print(f"[DataPulse-NB1] Email HTML built — {len(email_body):,} chars")

# COMMAND ----------
# MAGIC %md ## CELL 7 — Persist Results and Send Email

# COMMAND ----------

# ── Save Tracking Record ───────────────────────────────────────────────────────
analysis_summary = {
    "run_date":           today_str,
    "tables_analyzed":    len(tables),
    "views_analyzed":     len(views),
    "jobs_analyzed":      len(job_data),
    "small_file_tables":  len(small_files),
    "missing_cluster":    len(missing_cluster),
    "long_running_jobs":  len(long_running),
    "failing_jobs":       len(failing_jobs),
    "period_failures":    len(period_failures),
    "stale_tables":       len(stale_tables),
    "stale_views":        len(stale_views),
    "stale_jobs":         len(stale_jobs),
    "est_monthly_cost":   est_monthly,
    "est_savings":        est_savings,
}

tracking_row = Row(
    run_date=date.fromisoformat(today_str),
    analysis_json=json.dumps(analysis_summary),
    created_at=datetime.now(TZ),
)
spark.createDataFrame([tracking_row]).write.format("delta").mode("append").saveAsTable(TRACK_TBL)
print(f"[DataPulse-NB1] Tracking record saved: {analysis_summary}")

# ── Send Email ─────────────────────────────────────────────────────────────────
email_params = {
    "email_from":       EMAIL_FROM,
    "email_subject":    f"[DataPulse] Optimization Report — {today_str}",
    "email_body":       email_body,
    "email_to":         EMAIL_TO,
    "email_cc":         EMAIL_CC,
    "email_bcc":        "",
    "body_type":        "html",
    "attachment_paths": "",
    "chart_path":       "",
}

try:
    dbutils.notebook.run(EMAIL_NB, 300, email_params)
    print("[DataPulse-NB1] ✅ Email sent successfully.")
except Exception as exc:
    print(f"[DataPulse-NB1] ❌ Email send failed: {exc}")
    raise

print("[DataPulse-NB1] 🏁 Complete.")
