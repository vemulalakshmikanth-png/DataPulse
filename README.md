# 🔍 DataPulse — Databricks Monitoring Suite

> Automated nightly Databricks monitoring suite that detects schema changes,
> optimization issues, and data lineage impact — delivered as HTML email reports.

## What It Does

DataPulse is a two-notebook Databricks monitoring system that runs nightly at 11 PM ET:

- **Notebook 1 — Optimization Analysis**: Scans tables and jobs for small file issues,
  missing liquid clustering, long-running/failing jobs, and estimates cost savings.
- **Notebook 2 — Metadata Change & Lineage**: Detects daily schema changes, naming
  violations, stale data, and traces downstream impact via Unity Catalog lineage API.

Each notebook sends a rich HTML email report to configured distribution lists.

## Key Features

- 🔍 Detects schema changes, added/removed columns, and type changes daily
- ⚡ Identifies small file issues, missing liquid clustering, and cost savings
- 📉 Flags long-running and frequently failing Databricks jobs
- 🕐 Tracks data freshness using DESCRIBE HISTORY (not last_altered)
- 🔗 Traces downstream lineage via Unity Catalog API
- 📧 Sends styled HTML email reports with risk badges and recommended actions
- ♻️  Idempotent, gap-aware, and fully parameterized via job widgets

## Tech Stack

- Python / PySpark (Databricks Runtime)
- Databricks REST API (Jobs, Lineage, Workspace)
- Unity Catalog (system.access.table_lineage)
- Delta Lake (snapshot tables, tracking tables)
- HTML email via shared email notebook

## Environment Setup

| Config | Value |
|---|---|
| Catalog | source_catalog |
| Schemas | raw_schema, transform_schema |
| Storage | target_schema |
| Timezone | America/New_York |
| Schedule | Daily at 11 PM ET |

---
*Built for Databricks data platform monitoring and governance.*
