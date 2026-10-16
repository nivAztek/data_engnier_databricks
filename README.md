# Tel Aviv 2023 — Street Closure Compensation Pipeline

A production-grade **Medallion Architecture** (Bronze → Silver → Gold) PySpark pipeline for Tel Aviv Municipality data, built on **Databricks Lakeflow Spark Declarative Pipelines (SDP)** and structured as a **Declarative Automation Bundle (DAB)** for cross-workspace deployment.

---

## Architecture Overview

```
┌─────────────────────┐
│   External Sources   │
│  CSV (GCS) + API     │
└──────────┬──────────┘
           │  Job Task 1: Notebook (serverless)
           ▼
┌─────────────────────┐
│     UC Volume        │
│  /Volumes/.../raw_data │
│  ├── street_closures/│
│  └── businesses/     │
└──────────┬──────────┘
           │  Job Task 2: SDP Pipeline (serverless)
           ▼
┌─────────────────────────────────────────────────┐
│                  BRONZE LAYER                    │
│  Streaming Tables + Auto Loader                  │
│  ├── bronze_street_closures  (CSV → ST)          │
│  └── bronze_businesses       (JSON → ST)         │
│  DQ: expect_or_drop on required fields           │
└──────────────────────┬──────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────┐
│                  SILVER LAYER                    │
│  ├── silver_street_closures  (Streaming Table)   │
│  │   Date parsing, street explosion, normalization│
│  ├── silver_businesses       (Auto CDC SCD1)     │
│  │   Keyed on id_esek, sequenced by OBJECTID     │
│  ├── silver_dim_streets      (MV, reference)     │
│  └── silver_dim_closure_types (MV, reference)    │
│  DQ: expect_or_fail on dates, expect_all_or_drop │
└──────────────────────┬──────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────┐
│                   GOLD LAYER                     │
│  Star Schema with Central Fact                   │
│  ┌─────────────────────────────────────────┐     │
│  │  gold_fact_daily_compensation            │     │
│  │  Grain: (id_esek, closure_date)          │     │
│  │  1.37M rows — SINGLE source of truth     │     │
│  └────────────────┬────────────────────────┘     │
│                   ▼                              │
│  ├── gold_compensation_per_business (agg)        │
│  ├── gold_compensation_per_street   (agg)        │
│  ├── gold_monthly_compensation_trend (agg)       │
│  ├── gold_compensation_by_business_type (agg)    │
│  ├── gold_cost_per_area             (agg)        │
│  └── gold_closure_duration_by_street (agg)       │
│  DQ: expect positive_compensation                │
└─────────────────────────────────────────────────┘
```

## Why This Approach?

### 1. Streaming Tables + Auto Loader (Bronze)

**Why not materialized views?** MVs re-read the entire source on every refresh. With Auto Loader, bronze tables only process *new files* landed in the volume — making incremental runs fast and cost-efficient. As the dataset grows (daily snapshots accumulate), this becomes critical.

**Why a UC Volume landing zone?** Spark cannot read GCS HTTPS URLs directly. By decoupling HTTP I/O (notebook task) from the pipeline (Auto Loader), we achieve:
- **Idempotency**: The pipeline is deterministic — same files always produce the same result.
- **Replayability**: Re-run the pipeline without re-downloading data.
- **Separation of concerns**: Network failures don't crash the pipeline.

### 2. Auto CDC / SCD Type 1 (Silver Businesses)

**Why CDC instead of a window-function dedup?** Businesses change over time — a shop may move streets, change type, or update its area. Auto CDC (`dp.create_auto_cdc_flow`) with SCD Type 1:
- Automatically upserts by `id_esek`, keeping the latest state by `OBJECTID`.
- Handles out-of-order arrivals natively.
- Eliminates manual window/row_number deduplication.
- Supports future upgrade to SCD Type 2 if historical tracking is needed.

### 3. Streaming Table for Street Closures (Silver)

Closure events are **append-only facts** — once recorded, they don't change. A streaming table is the natural fit:
- New closure files are incrementally processed.
- The explode (both streets per closure) happens once, not recomputed each run.
- `cluster_by` on `(affected_street, closure_start)` optimizes downstream joins.

### 4. Central Fact Table (Gold)

**Why `gold_fact_daily_compensation` instead of direct joins in each aggregation?**

The previous architecture duplicated the expensive join (closures × businesses) and date explosion in both `compensation_per_business` and `monthly_trend`. The central fact:
- **Eliminates duplication**: One join, one date explosion, one dedup — all downstream MVs read from it.
- **Star schema**: Clean separation of fact (daily compensation) from dimensions (businesses, streets, closure types).
- **Performance**: 1.37M rows at `(id_esek, closure_date)` grain, clustered by `(closure_date, shem_rechov)`.

### 5. Dimension Tables (Silver)

- **`dim_streets`**: Reference table of all streets from both sources, with flags indicating which source mentions them. Enables street-level analytics without full table scans.
- **`dim_closure_types`**: Reference dimension for closure type codes. Small but important for filtering and categorization.

### 6. Data Quality Expectations

| Layer | Rule | Action |
|-------|------|--------|
| Bronze | `ID IS NOT NULL`, `me_shem_rechov IS NOT NULL`, `ad_shem_rechov IS NOT NULL`, `tr_from IS NOT NULL` | `expect_or_drop` — silently remove bad rows |
| Bronze | `id_esek IS NOT NULL`, `shem_rechov IS NOT NULL` | `expect_or_drop` — 15 records dropped |
| Silver | `closure_start IS NOT NULL`, `affected_street != ''` | `expect_or_fail` — halt on critical issues |
| Silver | `id_esek IS NOT NULL`, `shetach >= 0` | `expect_all_or_drop` (CDC target) |
| Gold | `daily_compensation_ils > 0` | `expect` — warn but don't drop |

### 7. Serverless Everything

Both job tasks run on serverless compute:
- **Ingestion notebook**: Serverless Python environment (no cluster provisioning).
- **Pipeline refresh**: Serverless SDP with Photon acceleration.
- **Result**: Sub-4-minute end-to-end runs with zero cluster management.

### 8. Pipeline Configuration

All business parameters are externalized via `spark.conf`:
- `pipeline.year_start` / `pipeline.year_end` — configurable analysis period
- `pipeline.volume_path` — environment-aware volume path
- Defaults provided for local development; overridden per target in the DAB.

## Compensation Business Rules

1. A business is eligible when its street appears in closure data.
2. Compensation is calculated **once per business per day** (no double-counting from overlapping closures).
3. Daily rate: `MIN(shetach × 100, 10000)` ILS.
4. Both streets in each closure row are affected (handled via explode in silver).
5. Only year 2023 (clamped to Jan 1 – Dec 31).

## DAB Project Structure

```
data_engnieer/
├── README.md                              # This file
├── databricks.yml                         # Bundle config + volume resource
├── dashboards/
│   └── compensation_analysis.lvdash.json  # Exported AI/BI dashboard
├── resources/
│   ├── tel_aviv_pipeline.pipeline.yml     # SDP pipeline with config params
│   ├── tel_aviv_job.job.yml               # 2-task job (ingest → pipeline)
│   └── tel_aviv_dashboard.dashboard.yml   # Dashboard resource
└── src/
    ├── _config.py                         # Shared config from spark.conf
    ├── ingest/
    │   └── download_sources/              # Notebook: HTTP → UC Volume
    ├── bronze/
    │   ├── street_closures.py             # ST + Auto Loader (CSV)
    │   └── businesses.py                  # ST + Auto Loader (JSON)
    ├── silver/
    │   ├── street_closures.py             # ST: date parsing + street explosion
    │   ├── businesses.py                  # Auto CDC SCD Type 1 dimension
    │   ├── dim_streets.py                 # MV: street reference
    │   └── dim_closure_types.py           # MV: closure type reference
    └── gold/
        ├── fact_daily_compensation.py     # Central fact (single join point)
        ├── agg_per_business.py            # MV from fact
        ├── agg_per_street.py              # MV from fact
        ├── agg_monthly_trend.py           # MV from fact
        ├── agg_by_business_type.py        # MV from fact
        ├── agg_cost_per_area.py           # MV from fact
        └── agg_closure_duration.py        # MV from silver closures
```

## Deployment

```bash
# Validate
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev --auto-approve

# Run the pipeline
databricks bundle run tel_aviv_compensation --target dev

# Deploy to production
databricks bundle deploy --target prod --auto-approve
```

## Data Sources

| Source | Type | Records | URL |
|--------|------|---------|-----|
| Street Closures | CSV (GCS) | 2,006 rows | `storage.googleapis.com/test_onedatai/rechov_sagur.csv` |
| Businesses | ArcGIS REST API | 37,360 features | `gisn.tel-aviv.gov.il/arcgis/rest/services/IView2/MapServer/925/query` |

## Pipeline Output (13 Datasets)

| Layer | Table | Type | Rows |
|-------|-------|------|------|
| Bronze | bronze_street_closures | Streaming Table | 2,006 |
| Bronze | bronze_businesses | Streaming Table | 37,345 |
| Silver | silver_street_closures | Streaming Table | 4,012 |
| Silver | silver_businesses | Streaming Table (CDC) | 37,345 |
| Silver | silver_dim_streets | Materialized View | 1,615 |
| Silver | silver_dim_closure_types | Materialized View | 26 |
| Gold | gold_fact_daily_compensation | Materialized View | 1,376,228 |
| Gold | gold_compensation_per_business | Materialized View | 11,813 |
| Gold | gold_compensation_per_street | Materialized View | 87 |
| Gold | gold_monthly_compensation_trend | Materialized View | 12 |
| Gold | gold_compensation_by_business_type | Materialized View | 165 |
| Gold | gold_cost_per_area | Materialized View | 11,813 |
| Gold | gold_closure_duration_by_street | Materialized View | 102 |
