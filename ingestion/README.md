# ingestion

dlt pipelines that load source data into the warehouse.

| Source | Pipeline | Status |
|---|---|---|
| NFZ "Terminy Leczenia" API v1.4 (`/queues`) | `nfz_queues` (`uv run python ingestion/nfz_queues.py`) | working: local DuckDB `raw_nfz.queues`, one snapshot per day, personal data stripped at extraction; `--target prod` writes to MotherDuck (token from `MOTHERDUCK_TOKEN`, not yet run) |
| Benefit → specialty mapping (Google Sheets) | Fivetran connector | planned |

## Running it

```bash
uv run python ingestion/nfz_queues.py                    # local DuckDB, replaces today's snapshot
uv run python ingestion/nfz_queues.py --skip-if-loaded   # keeps today's snapshot if it is already there
uv run python ingestion/nfz_queues.py --target prod      # MotherDuck (needs MOTHERDUCK_TOKEN)
```

A full read takes about 7 minutes (≈300 pages, one request per second). Until the daily GitHub Actions
run exists, the snapshot is taken by a local scheduler (macOS launchd) twice a day with `--skip-if-loaded`,
so the second slot only fills a day the first one missed. The API serves the current state only:
a day that is not read is lost.
