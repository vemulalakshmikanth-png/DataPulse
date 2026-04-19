# Databricks notebook source
# MAGIC %md
# MAGIC # 🧬 DataPulse — Metadata Change Notification & Data Lineage
# MAGIC
# MAGIC **Purpose:** Daily detection of schema changes, naming violations, stale data,
# MAGIC and downstream lineage impact across all monitored schemas.
# MAGIC
# MAGIC - Schema change detection (new/removed tables, added/removed/changed columns)
# MAGIC - Naming standard enforcement (raw_schema + transform_schema rules)
# MAGIC - Data freshness via DESCRIBE HISTORY (not last_altered)
# MAGIC - Unity Catalog lineage tracing for all impacted assets
# MAGIC - Professional HTML email with risk-rated impact analysis
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
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from typing import Optional

import pytz
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Widgets ────────────────────────────────────────────────────────────────────
dbutils.widgets.text("email_to", "your_email@domain.com", "Email To")
dbutils.widgets.text("email_cc", "",                       "Email CC")

EMAIL_TO = dbutils.widgets.get("email_to").strip()
EMAIL_CC = dbutils.widgets.get("email_cc").strip()

# ── Constants ──────────────────────────────────────────────────────────────────
CATALOG          = "source_catalog"
SCHEMAS          = ["raw_schema", "transform_schema"]
RAW_SCHEMA       = "raw_schema"
TRANSFORM_SCHEMA = "transform_schema"
DEST             = "target_schema"
COL_SNAP_TBL     = f"{DEST}.metadata_column_snapshot"
TBL_SNAP_TBL     = f"{DEST}.metadata_table_snapshot"
EMAIL_FROM       = "metadata_from_address@domain.com"
EMAIL_NB         = "/Repos/repo_path/email_notification_notebook"
REPO_PATH        = "/Repos/repo_path"
STALE_DATA_DAYS  = 7
TZ               = pytz.timezone("America/New_York")

WRITE_OPS = {
    "WRITE", "MERGE", "DELETE", "UPDATE", "STREAMING UPDATE",
    "COPY INTO", "CREATE TABLE AS SELECT",
    "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT",
}

# ── API Setup ──────────────────────────────────────────────────────────────────
WORKSPACE_HOST = spark.conf.get("spark.databricks.workspaceUrl")
API_BASE       = f"https://{WORKSPACE_HOST}"
TOKEN          = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext().apiToken().get()
)
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# ── Timestamps ─────────────────────────────────────────────────────────────────
now_et    = datetime.now(TZ)
today_str = now_et.strftime("%Y-%m-%d")

print(f"[DataPulse-NB2] Run date: {today_str}")


# ── API Helper ─────────────────────────────────────────────────────────────────
def _api_get(path: str, params: dict = None) -> dict:
    resp = requests.get(f"{API_BASE}{path}", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── Snapshot Tables ────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {COL_SNAP_TBL} (
        snapshot_date    STRING,
        table_schema     STRING,
        table_name       STRING,
        column_name      STRING,
        full_data_type   STRING,
        ordinal_position INT,
        is_nullable      STRING,
        col_comment      STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_SNAP_TBL} (
        snapshot_date      STRING,
        table_schema       STRING,
        table_name         STRING,
        table_type         STRING,
        data_source_format STRING,
        created            TIMESTAMP,
        last_altered       TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# ── Previous Snapshot (gap-handling) ──────────────────────────────────────────
prev_row = spark.sql(f"""
    SELECT MAX(snapshot_date) AS prev_date
    FROM   {TBL_SNAP_TBL}
    WHERE  snapshot_date < '{today_str}'
""").first()

prev_date    = str(prev_row["prev_date"]) if prev_row["prev_date"] else None
has_previous = prev_date is not None
print(f"[DataPulse-NB2] Previous snapshot: {prev_date or 'None — first run'}")

# COMMAND ----------
# MAGIC %md ## CELL 3 — Capture Current Metadata Snapshot

# COMMAND ----------

schemas_in_clause = ", ".join(f"'{s}'" for s in SCHEMAS)

# ── Column snapshot (no ORDER BY — skip unnecessary sort) ──────────────────────
curr_cols = spark.sql(f"""
    SELECT
        table_schema,
        table_name,
        column_name,
        full_data_type,
        ordinal_position,
        is_nullable,
        comment        AS col_comment,
        '{today_str}'  AS snapshot_date
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ({schemas_in_clause})
""").cache()

# ── Table snapshot ─────────────────────────────────────────────────────────────
curr_tbls = spark.sql(f"""
    SELECT
        table_schema,
        table_name,
        table_type,
        data_source_format,
        created,
        last_altered,
        '{today_str}' AS snapshot_date
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ({schemas_in_clause})
""").cache()

# Materialise caches
col_count = curr_cols.count()
tbl_count = curr_tbls.count()
print(f"[DataPulse-NB2] Current snapshot — columns: {col_count:,}  tables/views: {tbl_count:,}")

# COMMAND ----------
# MAGIC %md ## CELL 4 — Detect Changes, Naming Violations, and Stale Data

# COMMAND ----------

# ── Schema Change Detection ────────────────────────────────────────────────────
new_tables, removed_tables                         = [], []
added_cols, removed_cols, changed_cols             = [], [], []

if has_previous:
    prev_cols = spark.sql(f"""
        SELECT table_schema, table_name, column_name, full_data_type, is_nullable
        FROM   {COL_SNAP_TBL}
        WHERE  snapshot_date = '{prev_date}'
    """).cache()

    prev_tbls = spark.sql(f"""
        SELECT table_schema, table_name, table_type, data_source_format
        FROM   {TBL_SNAP_TBL}
        WHERE  snapshot_date = '{prev_date}'
    """).cache()

    # New tables
    new_tables = (
        curr_tbls
        .join(prev_tbls, ["table_schema", "table_name"], "left_anti")
        .select("table_schema", "table_name", "table_type", "data_source_format")
        .collect()
    )

    # Removed tables
    removed_tables = (
        prev_tbls
        .join(curr_tbls, ["table_schema", "table_name"], "left_anti")
        .select("table_schema", "table_name", "table_type", "data_source_format")
        .collect()
    )

    # Restrict column diffs to tables that exist in BOTH snapshots
    existing_keys = (
        curr_tbls
        .join(prev_tbls, ["table_schema", "table_name"], "inner")
        .select("table_schema", "table_name")
    )

    curr_cols_existing = curr_cols.join(existing_keys, ["table_schema", "table_name"], "inner")
    prev_cols_existing = prev_cols.join(existing_keys, ["table_schema", "table_name"], "inner")

    # Added columns
    added_cols = (
        curr_cols_existing
        .join(prev_cols_existing, ["table_schema", "table_name", "column_name"], "left_anti")
        .select("table_schema", "table_name", "column_name", "full_data_type", "is_nullable")
        .collect()
    )

    # Removed columns
    removed_cols = (
        prev_cols_existing
        .join(curr_cols_existing, ["table_schema", "table_name", "column_name"], "left_anti")
        .select("table_schema", "table_name", "column_name", "full_data_type", "is_nullable")
        .collect()
    )

    # Type / nullable changes
    changed_cols = (
        curr_cols_existing.alias("c")
        .join(
            prev_cols_existing.alias("p"),
            ["table_schema", "table_name", "column_name"],
            "inner",
        )
        .filter(
            (F.col("c.full_data_type") != F.col("p.full_data_type"))
            | (F.col("c.is_nullable")   != F.col("p.is_nullable"))
        )
        .select(
            F.col("c.table_schema"),
            F.col("c.table_name"),
            F.col("c.column_name"),
            F.col("p.full_data_type").alias("old_type"),
            F.col("c.full_data_type").alias("new_type"),
            F.col("p.is_nullable").alias("old_nullable"),
            F.col("c.is_nullable").alias("new_nullable"),
        )
        .collect()
    )

    prev_cols.unpersist()
    prev_tbls.unpersist()

print(f"[DataPulse-NB2] Changes — new:{len(new_tables)} removed:{len(removed_tables)} "
      f"cols+:{len(added_cols)} cols-:{len(removed_cols)} type_changes:{len(changed_cols)}")


# ── Naming Standard Violations ─────────────────────────────────────────────────

# raw_schema bad table names
raw_tbl_violations = (
    curr_tbls
    .filter(F.col("table_schema") == RAW_SCHEMA)
    .filter(
        (F.col("table_name") != F.lower(F.col("table_name")))
        | F.col("table_name").contains(" ")
    )
    .select("table_name",
            F.when(F.col("table_name") != F.lower(F.col("table_name")), "uppercase letters")
             .otherwise("contains spaces").alias("issue"))
    .collect()
)

# raw_schema bad column names
raw_col_violations = (
    curr_cols
    .filter(F.col("table_schema") == RAW_SCHEMA)
    .filter(
        (F.col("column_name") != F.lower(F.col("column_name")))
        | F.col("column_name").contains(" ")
    )
    .select("table_name", "column_name",
            F.when(F.col("column_name") != F.lower(F.col("column_name")), "uppercase letters")
             .otherwise("contains spaces").alias("issue"))
    .limit(50)
    .collect()
)

# transform_schema bad table names (must be lowercase AND end with _view)
xfm_tbl_violations = (
    curr_tbls
    .filter(F.col("table_schema") == TRANSFORM_SCHEMA)
    .filter(
        (F.col("table_name") != F.lower(F.col("table_name")))
        | ~F.col("table_name").endswith("_view")
    )
    .select("table_name", "table_type",
            F.when(F.col("table_name") != F.lower(F.col("table_name")), "uppercase letters")
             .when(~F.col("table_name").endswith("_view"), "missing _view suffix")
             .otherwise("multiple issues").alias("issue"))
    .limit(50)
    .collect()
)

# transform_schema governance: views not referencing publish_ or raw_schema
xfm_gov_violations = spark.sql(f"""
    SELECT table_name,
           SUBSTR(view_definition, 1, 200) AS definition_preview
    FROM   {CATALOG}.information_schema.views
    WHERE  table_schema = '{TRANSFORM_SCHEMA}'
      AND  view_definition NOT LIKE '%publish\\_%'
      AND  view_definition NOT LIKE '%{RAW_SCHEMA}%'
    LIMIT 30
""").collect()

print(f"[DataPulse-NB2] Naming violations — "
      f"raw_tbl:{len(raw_tbl_violations)} raw_col:{len(raw_col_violations)} "
      f"xfm_tbl:{len(xfm_tbl_violations)} gov:{len(xfm_gov_violations)}")


# ── Data Freshness Detection (DESCRIBE HISTORY) ───────────────────────────────
def check_table_freshness(schema: str, name: str) -> Optional[dict]:
    """
    Uses DESCRIBE HISTORY to find last data-write operation.
    Returns a freshness dict or None if table is not Delta or has no writes.
    """
    fqn = f"{CATALOG}.{schema}.{name}"
    try:
        hist = spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 30").collect()
    except Exception:
        return None   # Non-Delta table — skip silently

    write_ops = [r for r in hist if (r["operation"] or "").upper() in WRITE_OPS]
    if not write_ops:
        return {
            "schema":       schema,
            "name":         name,
            "fqn":          fqn,
            "fmt":          "DELTA",
            "last_write":   "No writes found",
            "last_op":      "—",
            "rows_written": "—",
            "days_since":   9999,
            "severity":     "CRITICAL",
        }

    latest     = write_ops[0]
    ts         = latest["timestamp"]
    ts_aware   = ts.replace(tzinfo=TZ) if ts.tzinfo is None else ts.astimezone(TZ)
    days_since = (now_et - ts_aware).days

    metrics    = latest.get("operationMetrics") or {}
    rows       = metrics.get("numOutputRows") or metrics.get("numTargetRowsInserted") or "—"

    if days_since > STALE_DATA_DAYS:
        if days_since > 90:   sev = "CRITICAL"
        elif days_since > 30: sev = "HIGH"
        elif days_since > 14: sev = "MEDIUM"
        else:                  sev = "LOW"

        return {
            "schema":       schema,
            "name":         name,
            "fqn":          fqn,
            "fmt":          "DELTA",
            "last_write":   ts_aware.strftime("%Y-%m-%d %H:%M ET"),
            "last_op":      latest["operation"],
            "rows_written": str(rows),
            "days_since":   days_since,
            "severity":     sev,
        }
    return None   # Fresh — no issue


# Only check base tables (not views)
base_tables = curr_tbls.filter(
    ~F.col("table_type").isin("VIEW", "MATERIALIZED_VIEW")
).select("table_schema", "table_name").collect()

stale_data: list[dict] = []
with ThreadPoolExecutor(max_workers=12) as pool:
    futures = {
        pool.submit(check_table_freshness, r.table_schema, r.table_name): r
        for r in base_tables
    }
    for fut in as_completed(futures):
        result = fut.result()
        if result:
            stale_data.append(result)

stale_data.sort(key=lambda x: -x["days_since"])
print(f"[DataPulse-NB2] Stale tables: {len(stale_data)}")
if stale_data:
    print(f"  Top 5: {[f'{s[\"schema\"]}.{s[\"name\"]} ({s[\"days_since\"]}d)' for s in stale_data[:5]]}")

# COMMAND ----------
# MAGIC %md ## CELL 5 — Collect Data Lineage for Changed Tables

# COMMAND ----------

# ── Build changes_by_table dict ────────────────────────────────────────────────
CHANGE_META = {
    "NEW TABLE":   {"color": "#2e7d32", "icon": "➕"},
    "REMOVED":     {"color": "#b71c1c", "icon": "✖"},
    "COL ADDED":   {"color": "#1565c0", "icon": "➕"},
    "COL REMOVED": {"color": "#b71c1c", "icon": "✖"},
    "TYPE CHANGE": {"color": "#e65100", "icon": "🔄"},
    "NAMING":      {"color": "#6a1b9a", "icon": "⚠"},
    "GOVERNANCE":  {"color": "#6a1b9a", "icon": "⚠"},
}

changes_by_table: dict[str, list[dict]] = {}

def _add_change(schema: str, name: str, ctype: str, col: str = "", detail: str = "") -> None:
    fqn = f"{CATALOG}.{schema}.{name}"
    changes_by_table.setdefault(fqn, []).append({
        "type":   ctype,
        "color":  CHANGE_META[ctype]["color"],
        "icon":   CHANGE_META[ctype]["icon"],
        "col":    col,
        "detail": detail,
    })

for r in new_tables:
    _add_change(r.table_schema, r.table_name, "NEW TABLE",
                detail=f"{r.table_type} | {r.data_source_format}")

for r in removed_tables:
    _add_change(r.table_schema, r.table_name, "REMOVED",
                detail=f"{r.table_type} | {r.data_source_format}")

for r in added_cols:
    _add_change(r.table_schema, r.table_name, "COL ADDED", col=r.column_name,
                detail=f"{r.full_data_type} | nullable={r.is_nullable}")

for r in removed_cols:
    _add_change(r.table_schema, r.table_name, "COL REMOVED", col=r.column_name,
                detail=f"{r.full_data_type}")

for r in changed_cols:
    _add_change(r.table_schema, r.table_name, "TYPE CHANGE", col=r.column_name,
                detail=f"{r.old_type} → {r.new_type} | nullable: {r.old_nullable} → {r.new_nullable}")

for r in raw_tbl_violations:
    _add_change(RAW_SCHEMA, r.table_name, "NAMING", detail=r.issue)

for r in xfm_tbl_violations:
    _add_change(TRANSFORM_SCHEMA, r.table_name, "NAMING", detail=r.issue)

for r in xfm_gov_violations:
    _add_change(TRANSFORM_SCHEMA, r.table_name, "GOVERNANCE",
                detail="View does not reference publish_ or raw_schema")

affected_fqns = list(changes_by_table.keys())
print(f"[DataPulse-NB2] Affected tables: {len(affected_fqns)}")


# ── UC Lineage API — parallelised ──────────────────────────────────────────────
def fetch_uc_lineage(fqn: str) -> tuple[str, dict]:
    try:
        data = _api_get(
            "/api/2.0/lineage-tracking/table-lineage",
            {"table_name": fqn, "include_entity_lineage": "true"},
        )
        downstreams = [d.get("name", "") for d in data.get("downstreams", [])[:15]]
        upstreams   = [u.get("name", "") for u in data.get("upstreams",   [])[:15]]
        notebooks   = [
            n.get("path", "") for n in data.get("entity_lineage", [])
            if n.get("entity_type") == "NOTEBOOK"
        ][:15]
        jobs = [
            j.get("name", "") for j in data.get("entity_lineage", [])
            if j.get("entity_type") == "JOB"
        ][:15]
        return fqn, {"downstreams": downstreams, "upstreams": upstreams,
                     "notebooks": notebooks, "jobs": jobs}
    except Exception as exc:
        print(f"  [WARN] Lineage API failed for {fqn}: {exc}")
        return fqn, {"downstreams": [], "upstreams": [], "notebooks": [], "jobs": []}


uc_lineage_map: dict[str, dict] = {}
with ThreadPoolExecutor(max_workers=10) as pool:
    for fqn, lineage in pool.map(fetch_uc_lineage, affected_fqns):
        uc_lineage_map[fqn] = lineage


# ── system.access.table_lineage (last 30 days) ─────────────────────────────────
access_lineage_map: dict[str, dict] = {}
if affected_fqns:
    fqns_sql = ", ".join(f"'{f}'" for f in affected_fqns)
    try:
        access_rows = spark.sql(f"""
            SELECT entity_type, entity_id, source_table_full_name
            FROM   system.access.table_lineage
            WHERE  source_table_full_name IN ({fqns_sql})
              AND  event_date >= DATE_SUB(CURRENT_DATE(), 30)
        """).collect()

        for r in access_rows:
            fqn = r.source_table_full_name
            access_lineage_map.setdefault(fqn, {"notebooks": [], "jobs": []})
            if r.entity_type == "NOTEBOOK":
                access_lineage_map[fqn]["notebooks"].append(str(r.entity_id))
            elif r.entity_type == "JOB":
                access_lineage_map[fqn]["jobs"].append(str(r.entity_id))
    except Exception as exc:
        print(f"  [WARN] system.access.table_lineage query failed: {exc}")


# ── Notebook ID Resolution ─────────────────────────────────────────────────────
raw_nb_ids: set[str] = set()
for v in access_lineage_map.values():
    raw_nb_ids.update(v.get("notebooks", []))

nb_id_to_path: dict[str, str] = {}

# Strategy 1: system.access.audit batch query
try:
    ids_sql = ", ".join(f"'{i}'" for i in raw_nb_ids)
    audit_rows = spark.sql(f"""
        SELECT request_params.notebookId AS nb_id,
               request_params.path       AS nb_path
        FROM   system.access.audit
        WHERE  action_name = 'getNotebook'
          AND  request_params.notebookId IN ({ids_sql})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY request_params.notebookId
                                   ORDER BY event_time DESC) = 1
    """).collect()
    for r in audit_rows:
        if r.nb_id and r.nb_path:
            nb_id_to_path[str(r.nb_id)] = r.nb_path
    print(f"[DataPulse-NB2] Audit strategy resolved: {len(nb_id_to_path)}/{len(raw_nb_ids)} notebooks")
except Exception:
    print("[DataPulse-NB2] Audit strategy failed (INSUFFICIENT_PERMISSIONS) — trying workspace scan")

# Strategy 2: Workspace List API recursive scan
remaining_ids = raw_nb_ids - set(nb_id_to_path.keys())
if remaining_ids:
    dir_count = [0]
    SCAN_TIMEOUT_S = 45

    def _scan_dir(path: str, depth: int, target_ids: set[str]) -> dict[str, str]:
        if depth <= 0 or dir_count[0] > 5000:
            return {}
        found = {}
        try:
            resp = _api_get("/api/2.0/workspace/list", {"path": path})
            for obj in resp.get("objects", []):
                oid = str(obj.get("object_id", ""))
                if oid in target_ids:
                    found[oid] = obj.get("path", "")
                if obj.get("object_type") == "DIRECTORY":
                    dir_count[0] += 1
                    found.update(_scan_dir(obj["path"], depth - 1, target_ids))
        except Exception:
            pass
        return found

    scan_paths = [
        (REPO_PATH, 4),
        (f"/Users", 3),
        (f"/Repos", 2),
        ("/Shared", 2),
    ]
    for scan_path, depth in scan_paths:
        if not remaining_ids:
            break
        try:
            with ThreadPoolExecutor(max_workers=4) as pool:
                fut = pool.submit(_scan_dir, scan_path, depth, remaining_ids)
                result = fut.result(timeout=SCAN_TIMEOUT_S)
                nb_id_to_path.update(result)
                remaining_ids -= set(result.keys())
        except FuturesTimeout:
            print(f"  [WARN] Workspace scan timed out on {scan_path}")

    print(f"[DataPulse-NB2] Workspace scan resolved: {len(nb_id_to_path)}/{len(raw_nb_ids)} notebooks")


# ── Job ID Resolution ──────────────────────────────────────────────────────────
raw_job_ids: set[str] = set()
for v in access_lineage_map.values():
    raw_job_ids.update(v.get("jobs", []))

def _resolve_job(jid: str) -> tuple[str, str]:
    try:
        data = _api_get(f"/api/2.1/jobs/get", {"job_id": jid})
        return jid, data.get("settings", {}).get("name", f"Job #{jid}")
    except Exception:
        return jid, f"Job #{jid}"

job_id_to_name: dict[str, str] = {}
with ThreadPoolExecutor(max_workers=10) as pool:
    for jid, jname in pool.map(_resolve_job, raw_job_ids):
        job_id_to_name[jid] = jname


# ── Merge lineage into unified map ─────────────────────────────────────────────
def nb_display_name(nb_id: str) -> str:
    path = nb_id_to_path.get(str(nb_id))
    if path:
        return path.rstrip("/").split("/")[-1]
    return f"Notebook #{nb_id}"

def job_display_name(jid: str) -> str:
    return job_id_to_name.get(str(jid), f"Job #{jid}")

lineage_map: dict[str, dict] = {}
for fqn in affected_fqns:
    uc  = uc_lineage_map.get(fqn, {})
    acc = access_lineage_map.get(fqn, {})

    all_nbs  = list({*uc.get("notebooks", []), *[nb_display_name(i) for i in acc.get("notebooks", [])]})
    all_jobs = list({*uc.get("jobs", []),      *[job_display_name(i) for i in acc.get("jobs", [])]})

    lineage_map[fqn] = {
        "downstreams": uc.get("downstreams", []),
        "upstreams":   uc.get("upstreams",   []),
        "notebooks":   all_nbs,
        "jobs":        all_jobs,
    }

print(f"[DataPulse-NB2] Lineage map built for {len(lineage_map)} tables")

# COMMAND ----------
# MAGIC %md ## CELL 6 — Generate HTML Email

# COMMAND ----------

# ── HTML Helpers (green theme) ─────────────────────────────────────────────────
RISK_COLORS = {
    "CRITICAL": "#b71c1c",
    "HIGH":     "#d84315",
    "MEDIUM":   "#ef6c00",
    "LOW":      "#d0d7de",
}

def badge(text: str, bg: str = "#607d8b", fg: str = "#fff") -> str:
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:12px;font-size:11px;font-weight:600;'
        f'white-space:nowrap">{html.escape(str(text))}</span>'
    )

def severity_badge(sev: str) -> str:
    return badge(sev, RISK_COLORS.get(sev, "#607d8b"))

def html_tbl(headers: list[str], rows: list[list]) -> str:
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

def code_tag(text: str) -> str:
    return (
        f'<code style="background:#f0f4f8;color:#1a3c5e;padding:1px 5px;'
        f'border-radius:3px;font-size:11px">{html.escape(str(text))}</code>'
    )

def nb_tag(name: str) -> str:
    return badge(name, "#e3f2fd", "#0d47a1")

def job_tag(name: str) -> str:
    return badge(name, "#fff8e1", "#e65100")

def section_header(title: str, icon: str = "") -> str:
    return (
        f'<h2 style="color:#1a4d3e;font-size:16px;font-weight:700;'
        f'border-bottom:2px solid #1a4d3e;padding-bottom:6px;margin-top:28px">'
        f"{icon} {html.escape(title)}</h2>"
    )

def dashboard_card(label: str, value, bg: str) -> str:
    return (
        f'<td style="background:{bg};padding:16px 20px;text-align:center;'
        f'border-radius:8px;min-width:100px">'
        f'<div style="font-size:26px;font-weight:800;color:#1a3c5e">{value}</div>'
        f'<div style="font-size:11px;color:#37474f;margin-top:4px">{html.escape(label)}</div>'
        f"</td>"
    )

def risk_level(fqn: str) -> str:
    changes = changes_by_table.get(fqn, [])
    lin     = lineage_map.get(fqn, {})
    n_ds    = len(lin.get("downstreams", []))
    breaking = any(c["type"] in ("COL REMOVED", "TYPE CHANGE") for c in changes)
    if breaking and n_ds >= 3:   return "CRITICAL"
    if breaking and n_ds >= 1:   return "HIGH"
    if n_ds >= 1:                 return "MEDIUM"
    return "LOW"


# ── Build Email ────────────────────────────────────────────────────────────────
def build_email() -> str:
    parts = []

    total_standards_issues = (len(raw_tbl_violations) + len(raw_col_violations)
                               + len(xfm_tbl_violations) + len(xfm_gov_violations))
    total_schema_changes   = len(new_tables) + len(removed_tables) + len(added_cols) + len(removed_cols) + len(changed_cols)
    affected_assets_count  = sum(
        len(v.get("downstreams", [])) + len(v.get("notebooks", [])) + len(v.get("jobs", []))
        for v in lineage_map.values()
    )

    # ── Header banner ─────────────────────────────────────────────────────────
    parts.append(f"""
    <div style="background:linear-gradient(135deg,#1a4d3e,#2a7a5f);
                padding:28px 32px;border-radius:10px 10px 0 0">
      <div style="font-size:24px;font-weight:800;color:#ffffff">
        🧬 DataPulse — Metadata Changes & Lineage
      </div>
      <div style="color:#b2dfdb;font-size:13px;margin-top:6px">
        {today_str}
        {'&nbsp;|&nbsp; Comparing against: ' + prev_date if has_previous else '&nbsp;|&nbsp; First run — baseline snapshot created'}
      </div>
    </div>
    """)

    # ── Dashboard cards ────────────────────────────────────────────────────────
    parts.append(f"""
    <table style="width:100%;border-spacing:8px;margin:16px 0"><tr>
      {dashboard_card("New Tables/Views",     len(new_tables),          "#e8f5e9")}
      {dashboard_card("Removed",              len(removed_tables),       "#ffebee")}
      {dashboard_card("Cols Added",           len(added_cols),           "#e3f2fd")}
      {dashboard_card("Cols Modified",        len(removed_cols)+len(changed_cols), "#fff8e1")}
      {dashboard_card("Standards Issues",     total_standards_issues,   "#f3e5f5")}
      {dashboard_card("Affected Assets",      affected_assets_count,    "#e0f2f1")}
      {dashboard_card("Stale Tables",         len(stale_data),          "#fff3e0")}
    </tr></table>
    """)

    # ── First run handling ─────────────────────────────────────────────────────
    if not has_previous:
        parts.append("""
        <div style="background:#e8f5e9;border-left:4px solid #2e7d32;
                    padding:12px 16px;border-radius:4px;margin:12px 0">
          ✅ <strong>Baseline snapshot created.</strong>
          Schema change detection will begin on the next run.
        </div>
        """)

    # ── Schema Changes ─────────────────────────────────────────────────────────
    if total_schema_changes > 0:
        if new_tables:
            parts.append(section_header("New Tables / Views", "➕"))
            rows = [
                [
                    badge("NEW", "#2e7d32"),
                    f'<code style="font-size:11px">{r.table_schema}.{r.table_name}</code>',
                    r.table_type,
                    r.data_source_format or "—",
                ]
                for r in new_tables
            ]
            parts.append(html_tbl(["Status", "Table/View", "Type", "Format"], rows))

        if removed_tables:
            parts.append(section_header("Tables / Views Removed", "✖"))
            rows = [
                [
                    badge("REMOVED", "#b71c1c"),
                    f'<code style="font-size:11px">{r.table_schema}.{r.table_name}</code>',
                    r.table_type,
                    r.data_source_format or "—",
                ]
                for r in removed_tables
            ]
            parts.append(html_tbl(["Status", "Table/View", "Type", "Format"], rows))

        if added_cols:
            parts.append(section_header("Columns Added", "➕"))
            rows = [
                [
                    f'<code style="font-size:11px">{r.table_schema}.{r.table_name}</code>',
                    badge(r.column_name, "#1565c0"),
                    r.full_data_type,
                    r.is_nullable,
                ]
                for r in added_cols
            ]
            parts.append(html_tbl(["Table", "Column", "Data Type", "Nullable"], rows))

        if removed_cols:
            parts.append(section_header("Columns Removed", "✖"))
            rows = [
                [
                    f'<code style="font-size:11px">{r.table_schema}.{r.table_name}</code>',
                    badge(r.column_name, "#b71c1c"),
                    r.full_data_type,
                    r.is_nullable,
                ]
                for r in removed_cols
            ]
            parts.append(html_tbl(["Table", "Column", "Data Type", "Nullable"], rows))

        if changed_cols:
            parts.append(section_header("Column Type / Nullable Changes", "🔄"))
            rows = [
                [
                    f'<code style="font-size:11px">{r.table_schema}.{r.table_name}</code>',
                    f'<code style="font-size:11px">{r.column_name}</code>',
                    f'{html.escape(r.old_type)} &rarr; <strong>{html.escape(r.new_type)}</strong>',
                    f'{r.old_nullable} &rarr; <strong>{r.new_nullable}</strong>',
                ]
                for r in changed_cols
            ]
            parts.append(html_tbl(["Table", "Column", "Type Change", "Nullable Change"], rows))

    # ── Data Lineage — Detailed Impact Analysis ────────────────────────────────
    if affected_fqns:
        parts.append(section_header("Data Lineage — Detailed Impact Analysis", "🔗"))

        # Summary table
        summary_rows = []
        for fqn in affected_fqns:
            lin     = lineage_map.get(fqn, {})
            changes = changes_by_table.get(fqn, [])
            risk    = risk_level(fqn)
            change_summary = ", ".join(sorted({c["type"] for c in changes}))
            summary_rows.append([
                code_tag(fqn),
                html.escape(change_summary),
                str(len(lin.get("downstreams", []))),
                str(len(lin.get("notebooks",   []))),
                str(len(lin.get("jobs",         []))),
                severity_badge(risk),
            ])
        parts.append(html_tbl(
            ["Changed Table", "Schema Changes", "Downstream", "Notebooks", "Jobs", "Risk"],
            summary_rows,
        ))

        # Per-table detail cards
        for fqn in affected_fqns:
            lin     = lineage_map.get(fqn, {})
            changes = changes_by_table.get(fqn, [])
            risk    = risk_level(fqn)
            border  = RISK_COLORS.get(risk, "#607d8b")

            change_items = "".join(
                f'<li style="margin:4px 0">'
                f'{c["icon"]} {badge(c["type"], c["color"])} '
                f'{f"<code style=\"font-size:11px\">{html.escape(c[\"col\"])}</code> — " if c["col"] else ""}'
                f'<span style="font-size:12px;color:#37474f">{html.escape(c["detail"])}</span>'
                f'</li>'
                for c in changes
            )

            ds_tags  = " ".join(code_tag(t) for t in lin.get("downstreams", []))
            us_tags  = " ".join(code_tag(t) for t in lin.get("upstreams",   []))
            nb_tags  = " ".join(nb_tag(n)   for n in lin.get("notebooks",   []))
            job_tags = " ".join(job_tag(j)  for j in lin.get("jobs",        []))

            parts.append(f"""
            <div style="border:2px solid {border};border-radius:8px;padding:14px 18px;
                        margin:12px 0;background:#fff">
              <div style="font-weight:700;font-size:13px;margin-bottom:8px">
                {code_tag(fqn)} &nbsp; {severity_badge(risk)}
              </div>
              <div style="font-size:12px;font-weight:700;color:#1a4d3e;margin-top:10px">What Changed</div>
              <ul style="margin:4px 0 10px;padding-left:20px">{change_items}</ul>
              <div style="font-size:12px;font-weight:700;color:#1a4d3e">Affected Assets</div>
              <div style="margin-top:4px;font-size:12px">
                {'<strong>Downstream:</strong> ' + ds_tags if ds_tags else ''}
                {'<br><strong>Upstream:</strong> '   + us_tags  if us_tags  else ''}
                {'<br><strong>Notebooks:</strong> '  + nb_tags  if nb_tags  else ''}
                {'<br><strong>Jobs:</strong> '        + job_tags if job_tags else ''}
                {'<em style="color:#888">No downstream assets found.</em>'
                 if not any([ds_tags, us_tags, nb_tags, job_tags]) else ''}
              </div>
            </div>
            """)

        # Recommended Actions
        parts.append(section_header("Recommended Actions", "📋"))
        actions = [
            f"Review and validate {len(removed_cols)} removed columns — downstream consumers may break",
            f"Test {len(changed_cols)} type/nullable changes across downstream pipelines",
            f"Update downstream ETL jobs for {len(added_cols)} newly added columns as needed",
            f"Notify owners of {len(lineage_map)} downstream tables of upstream changes",
            f"Review {len(set(n for v in lineage_map.values() for n in v.get('notebooks', [])))} affected notebooks for compatibility",
            f"Check {len(set(j for v in lineage_map.values() for j in v.get('jobs', [])))} affected jobs for schema drift errors",
            f"Fix {total_standards_issues} naming/governance violations to maintain data catalogue hygiene",
        ]
        alist = "".join(
            f'<li style="margin:6px 0;font-size:13px">'
            f'{badge(str(i + 1), "#1a4d3e")} {html.escape(a)}</li>'
            for i, a in enumerate(actions)
        )
        parts.append(f'<ol style="list-style:none;padding:0">{alist}</ol>')

    # ── Stale Data Section ─────────────────────────────────────────────────────
    if stale_data:
        parts.append(section_header("Stale Data Tables", "🕐"))
        parts.append("<p style='font-size:12px;color:#37474f'>"
                     "Based on DESCRIBE HISTORY — actual data write operations only. "
                     "Views are excluded (they don't store data).</p>")
        rows = [
            [
                code_tag(f'{s["schema"]}.{s["name"]}'),
                s["fmt"],
                s["last_write"],
                html.escape(s["last_op"]),
                s["rows_written"],
                badge(f'{s["days_since"]}d', "#78909c"),
                severity_badge(s["severity"]),
            ]
            for s in stale_data[:50]
        ]
        parts.append(html_tbl(
            ["Table", "Format", "Last Data Write", "Last Operation",
             "Rows Written", "Age", "Severity"],
            rows,
        ))

    # ── Naming Standards Violations ────────────────────────────────────────────
    if total_standards_issues > 0:
        parts.append(section_header("Naming Standards Violations", "⚠️"))

        if raw_tbl_violations:
            rows = [[code_tag(r.table_name), html.escape(r.issue)]
                    for r in raw_tbl_violations]
            parts.append("<p style='font-size:12px;font-weight:600'>raw_schema — Bad Table Names</p>")
            parts.append(html_tbl(["Table Name", "Issues"], rows))

        if raw_col_violations:
            rows = [[code_tag(r.table_name), code_tag(r.column_name), html.escape(r.issue)]
                    for r in raw_col_violations]
            parts.append("<p style='font-size:12px;font-weight:600'>raw_schema — Bad Column Names</p>")
            parts.append(html_tbl(["Table", "Column Name", "Issues"], rows))

        if xfm_tbl_violations:
            rows = [[code_tag(r.table_name), r.table_type, html.escape(r.issue)]
                    for r in xfm_tbl_violations]
            parts.append("<p style='font-size:12px;font-weight:600'>transform_schema — Bad Table Names</p>")
            parts.append(html_tbl(["Table Name", "Type", "Issues"], rows))

        if xfm_gov_violations:
            rows = [[code_tag(r.table_name),
                     f'<code style="font-size:10px">{html.escape(str(r.definition_preview))[:120]}…</code>']
                    for r in xfm_gov_violations]
            parts.append("<p style='font-size:12px;font-weight:600'>"
                         "transform_schema — Views Not Referencing publish_/raw_schema</p>")
            parts.append(html_tbl(["View Name", "Definition Preview"], rows))

    # ── Footer ─────────────────────────────────────────────────────────────────
    schemas_str = ", ".join(SCHEMAS)
    parts.append(f"""
    <div style="background:#eef1f5;padding:12px 20px;border-radius:0 0 10px 10px;
                margin-top:24px;font-size:11px;color:#555;text-align:center">
      Generated by <strong>DataPulse</strong> on {now_et.strftime('%Y-%m-%d %H:%M ET')}
      &nbsp;|&nbsp; Schemas: {schemas_str}
      &nbsp;|&nbsp; Changes: {total_schema_changes}
      &nbsp;|&nbsp; Violations: {total_standards_issues}
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
print(f"[DataPulse-NB2] Email HTML built — {len(email_body):,} chars")

# COMMAND ----------
# MAGIC %md ## CELL 7 — Save Snapshot and Send Email

# COMMAND ----------

# ── Idempotent Snapshot Save (INSERT INTO from SQL, not from cached DataFrames) ─
spark.sql(f"DELETE FROM {COL_SNAP_TBL} WHERE snapshot_date = '{today_str}'")
spark.sql(f"DELETE FROM {TBL_SNAP_TBL} WHERE snapshot_date = '{today_str}'")

spark.sql(f"""
    INSERT INTO {COL_SNAP_TBL}
    SELECT
        table_schema,
        table_name,
        column_name,
        full_data_type,
        ordinal_position,
        is_nullable,
        comment       AS col_comment,
        '{today_str}' AS snapshot_date
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ({", ".join(f"'{s}'" for s in SCHEMAS)})
""")

spark.sql(f"""
    INSERT INTO {TBL_SNAP_TBL}
    SELECT
        table_schema,
        table_name,
        table_type,
        data_source_format,
        created,
        last_altered,
        '{today_str}' AS snapshot_date
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ({", ".join(f"'{s}'" for s in SCHEMAS)})
""")

print(f"[DataPulse-NB2] Snapshots saved for {today_str}")

# ── Release Caches ─────────────────────────────────────────────────────────────
curr_cols.unpersist()
curr_tbls.unpersist()

# ── 90-day Retention Cleanup ───────────────────────────────────────────────────
cutoff_90d = (now_et - timedelta(days=90)).strftime("%Y-%m-%d")
spark.sql(f"DELETE FROM {COL_SNAP_TBL} WHERE snapshot_date < '{cutoff_90d}'")
spark.sql(f"DELETE FROM {TBL_SNAP_TBL} WHERE snapshot_date < '{cutoff_90d}'")
spark.sql(f"OPTIMIZE {COL_SNAP_TBL}")
spark.sql(f"OPTIMIZE {TBL_SNAP_TBL}")
print(f"[DataPulse-NB2] Cleanup complete — retained snapshots >= {cutoff_90d}")

# ── Send Email ─────────────────────────────────────────────────────────────────
email_params = {
    "email_from":       EMAIL_FROM,
    "email_subject":    f"[DataPulse] Metadata Changes & Lineage — {today_str}",
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
    print("[DataPulse-NB2] ✅ Email sent successfully.")
except Exception as exc:
    print(f"[DataPulse-NB2] ❌ Email send failed: {exc}")
    raise

print("[DataPulse-NB2] 🏁 Complete.")
