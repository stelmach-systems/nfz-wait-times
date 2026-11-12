# nfz-wait-times

Daily-snapshot ELT pipeline for Polish public healthcare waiting times.

The NFZ (National Health Fund) API publishes only the **current** forecast of how long patients wait for each treatment at each provider. It keeps no history. This project snapshots it every day and builds that history, then models it with dbt to answer: *which treatments have the longest waits, how do forecasts change over time, and how fresh is the data?*

> **Status: work in progress.** The dlt source, the first dbt staging model and a local Airflow DAG work; cloud, CI and BI are next. The README is updated as each part lands.

## Planned architecture

```text
NFZ API v1.4 ──dlt──┐
                    ├──> DuckDB (dev) / MotherDuck (prod) ──dbt──> marts ──> BI
Google Sheets ──Fivetran──┘
```

| Layer | Tool | Status |
|---|---|---|
| Ingestion (code) | dlt | working locally |
| Ingestion (managed) | Fivetran | planned |
| Warehouse | DuckDB locally, MotherDuck in prod | planned |
| Transformation | dbt Core 1.12 (dbt-duckdb) | staging (`stg_nfz__queues`) with tests |
| CI + daily schedule | GitHub Actions | planned |
| Orchestration | Airflow (Astro CLI, local) | working locally: DAG `nfz_daily`, see `orchestration/` |
| BI | Lightdash, Looker Studio | planned |

## Quick start

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run python ingestion/nfz_queues.py   # ~7 min, writes data/nfz.duckdb
cd transform && uv run dbt build        # staging model, data tests, unit test of the pcus parser
```

## Data and licence

Source data: NFZ "Terminy Leczenia" API, published on [dane.gov.pl](https://dane.gov.pl/pl/dataset/1455) under **CC BY 4.0**. Source: Narodowy Fundusz Zdrowia.
