# ingestion

dlt pipelines that load source data into the warehouse.

| Source | Pipeline | Status |
|---|---|---|
| NFZ "Terminy Leczenia" API v1.4 (`/queues`) | `nfz_queues` (`uv run python ingestion/nfz_queues.py`) | working: local DuckDB `raw_nfz.queues`, one snapshot per day, personal data stripped at extraction |
| Benefit → specialty mapping (Google Sheets) | Fivetran connector | planned |
