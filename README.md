# 🔍 DataPulse — Databricks Monitoring Suite

> Automated nightly Databricks monitoring suite that detects schema changes,
> optimization issues, and data lineage impact — delivered as professional HTML email reports.

[!\[Databricks](https://img.shields.io/badge/Databricks-PySpark-orange?logo=databricks)](https://databricks.com)
[!\[Delta Lake](https://img.shields.io/badge/Delta%20Lake-Enabled-blue)](https://delta.io)
[!\[Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Lineage-green)](https://databricks.com/product/unity-catalog)
\[!\[Schedule](https://img.shields.io/badge/Schedule-11PM%20ET%20Daily-informational)]()

\---

## 📋 What It Does

DataPulse is a production-grade, two-notebook Databricks monitoring system that runs nightly at 11 PM ET. It scans your entire data platform and delivers actionable HTML email reports to configured distribution lists — with zero manual intervention.

### Notebook 1 — Optimization Analysis

Scans all tables, views, and jobs for performance issues and cost savings opportunities:

|Check|Description|
|-|-|
|⚡ Small Files|Tables with >50 files AND avg <32MB, or >200 files. Generates OPTIMIZE commands|
|🔗 Missing Clustering|Tables >100MB with no liquid clustering. Auto-recommends clustering columns|
|⏱ Long-running Jobs|Jobs with avg runtime >10 min. Estimates monthly cost and savings|
|❌ Failing Jobs|Jobs with >20% fail rate. Provides diagnostic templates|
|🕐 Stale Assets|Tables/views/jobs not updated within 60 days|
|💰 Cost Estimates|Monthly DBU cost + potential savings with 30% optimization|

### Notebook 2 — Metadata Change \& Data Lineage

Compares daily metadata snapshots to surface governance issues and lineage impact:

|Check|Description|
|-|-|
|🧬 Schema Changes|New/removed tables, added/removed/changed columns with old→new detail|
|🔗 Lineage Tracing|UC Lineage API + system.access.table\_lineage for downstream impact|
|⚠ Naming Standards|raw\_schema (lowercase, no spaces) + transform\_schema (\_view suffix)|
|🏛 View Governance|transform\_schema views must reference publish\_ or raw\_schema|
|🕐 Data Freshness|DESCRIBE HISTORY per table (not last\_altered) for true write detection|
|🎯 Risk Rating|CRITICAL / HIGH / MEDIUM / LOW per changed table based on lineage depth|

\---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DataPulse Suite                         │
│                                                             │
│  ┌──────────────────┐        ┌──────────────────────────┐   │
│  │   Notebook 1     │        │      Notebook 2          │   │
│  │  Optimization    │        │  Metadata + Lineage      │   │
│  │  Analysis        │        │  Change Notification     │   │
│  └────────┬─────────┘        └───────────┬──────────────┘   │
│           │                              │                  │
│     ┌─────▼──────────────────────────────▼──────┐           │
│     │         Email Notification Notebook       │           │
│     └───────────────────────────────────────────┘           │
│                                                             │
│  Delta Tables:  optimization\_run\_tracker                  │
│                 metadata\_column\_snapshot                  │
│                 metadata\_table\_snapshot                   │
└─────────────────────────────────────────────────────────────┘
```

\---

## ⚙️ Environment Setup

Update the constants in **Cell 2** of each notebook before deploying:

|Parameter|Value|Notes|
|-|-|-|
|`CATALOG`|`source\_catalog`|Unity Catalog catalog name|
|`SCHEMAS`|`\["raw\_schema", "transform\_schema"]`|Schemas to monitor|
|`DEST`|`target\_schema`|Where Delta tracking tables are created|
|`EMAIL\_NB`|`/Repos/repo\_path/email\_notification\_notebook`|Shared email notebook path|
|`REPO\_PATH`|`/Repos/repo\_path`|Repo root for notebook ID resolution|
|`TZ`|`America/New\_York`|Timezone for all timestamps|
|`EXCLUDED\_JOBS`|`{"UC\_ON\_DEMAND\_SYNC"}`|System jobs to skip|
|`STALE\_DAYS`|`60`|Days before an asset is considered stale (NB1)|
|`STALE\_DATA\_DAYS`|`7`|Days before a table's data is considered stale (NB2)|

\---

## 🚀 Deployment

### 1\. Upload Notebooks

Upload both `.py` files to your Databricks workspace repo:

```
/Repos/your\_repo/DataPulse/
├── notebook1\_optimization.py
├── notebook2\_metadata.py
└── email\_notification\_notebook  (shared — configure separately)
```

### 2\. Create Databricks Jobs

Create two jobs with the following settings:

**Job 1 — DataPulse Optimization**

```json
{
  "name": "DataPulse — Optimization Analysis",
  "schedule": {
    "quartz\_cron\_expression": "0 0 23 \* \* ?",
    "timezone\_id": "America/New\_York"
  },
  "parameters": \[
    {"name": "email\_to", "default": "your-team@domain.com"},
    {"name": "email\_cc", "default": ""}
  ]
}
```

**Job 2 — DataPulse Metadata**

```json
{
  "name": "DataPulse — Metadata Change \& Lineage",
  "schedule": {
    "quartz\_cron\_expression": "0 0 23 \* \* ?",
    "timezone\_id": "America/New\_York"
  },
  "parameters": \[
    {"name": "email\_to", "default": "your-team@domain.com"},
    {"name": "email\_cc", "default": ""}
  ]
}
```

> \*\*Tip:\*\* Set `email\_to` and `email\_cc` as job parameters so distribution lists can be changed without editing notebook code.

### 3\. Required Permissions

|Permission|Required For|
|-|-|
|`SELECT` on `information\_schema`|Table/column metadata|
|`SELECT` on `system.access.table\_lineage`|Lineage enrichment|
|`SELECT` on `system.access.audit`|Notebook ID resolution (Strategy 1)|
|`WRITE` on `target\_schema`|Delta tracking/snapshot tables|
|`READ` on all monitored schemas|DESCRIBE DETAIL, DESCRIBE HISTORY|
|Databricks Jobs API access|Job metrics and failure collection|
|Databricks Workspace API access|Notebook path resolution|

\---

## 🎨 Email Design

### Notebook 1 — Navy Theme

|Element|Color|
|-|-|
|Header gradient|`#1a3c5e → #2c5f8a`|
|Badge: CRITICAL|`#b71c1c`|
|Badge: HIGH|`#d84315`|
|Badge: MEDIUM|`#ef6c00`|
|Badge: GOOD|`#2e7d32`|

### Notebook 2 — Green Theme

|Element|Color|
|-|-|
|Header gradient|`#1a4d3e → #2a7a5f`|
|Risk: CRITICAL border|`#b71c1c`|
|Notebook tags|`#e3f2fd / #0d47a1`|
|Job tags|`#fff8e1 / #e65100`|
|Naming violations|`#6a1b9a`|

\---

## 🔧 Design Principles

|Principle|Implementation|
|-|-|
|**Idempotent**|Snapshot saves DELETE today's rows first, then INSERT — safe to re-run|
|**Gap-aware**|Tracks last successful run; extends lookback if a day is missed|
|**90-day retention**|Old snapshots auto-deleted and compacted nightly|
|**Parameterised**|`email\_to` / `email\_cc` overrideable per job run via widgets|
|**No ORDER BY on cache**|Skips unnecessary sort stage on cached DataFrames|
|**INSERT INTO from SQL**|Avoids Spark RPC serialization limits on large DataFrames|
|**Auto-optimize**|All Delta tables have `optimizeWrite` + `autoCompact` enabled|
|**Parallelised**|API calls use `ThreadPoolExecutor` (max\_workers=10–20)|

\---

## 📁 Repository Structure

```
DataPulse/
├── notebook1\_optimization.py       # NB1: Optimization Analysis
├── notebook2\_metadata.py           # NB2: Metadata Change \& Lineage
└── README.md
```

\---

## 🛣 Roadmap

* \[ ] Slack / Teams alert integration
* \[ ] Grafana dashboard for trend visualisation
* \[ ] Auto-apply OPTIMIZE recommendations via approval workflow
* \[ ] Multi-catalog support
* \[ ] Cost anomaly detection with ML baseline

\---

*Built for production Databricks data platform monitoring and governance.*

