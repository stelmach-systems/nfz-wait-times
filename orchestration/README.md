# orchestration

Local Apache Airflow (Astro CLI, Astro Runtime 3.3 = Airflow 3.3) running the pipeline as one DAG:

```text
ingest_nfz_queues  ──>  dbt_build  ──>  dbt_source_freshness
 (dlt, NFZ API)        (stg models,       (is the source
                         all tests)          still fresh?)
```

`dags/nfz_daily.py` runs daily at 09:30 Europe/Warsaw with `catchup=False`: the NFZ API serves only
the current state, so there is nothing to backfill.

## How the container gets the pipeline

- **Code** (`../ingestion`, `../transform`, `../.dlt`) is mounted read-only into the scheduler, where
  tasks run (`docker-compose.override.yml`). The DAG always runs the code from the repo.
- **Dependencies** live in a separate virtualenv inside the image (`/usr/local/airflow/nfz_venv`),
  installed with hashes from `requirements-nfz.txt`, which is exported from the repo's `uv.lock`.
  Airflow's own packages and dbt never share an environment.
- **Data**: Airflow writes its own DuckDB file, `../data-airflow/nfz.duckdb` (gitignored), never
  `../data/nfz.duckdb`. DuckDB allows one writer per file, but its file lock does not cross the
  Colima (virtiofs) mount, so a host process and a container could otherwise write the same file at once.
- dbt writes `target/` and logs to `/tmp/dbt` in the container, because the project mount is read-only.

## Run it

Requires the Astro CLI and a Docker-compatible runtime (here: a dedicated Colima profile).

```bash
colima start airflow                 # 4 CPU / 8 GB VM, separate from the default profile
mkdir -p data-airflow                # from the repo root: Airflow's own DuckDB file lives here
cd orchestration && astro dev start  # UI on http://localhost:8080 (bound to 127.0.0.1)
astro dev run dags trigger nfz_daily # or the ▶ button in the UI
astro dev stop && colima stop airflow
```

After changing `../pyproject.toml`, regenerate the requirements and rebuild:

```bash
uv export --frozen --no-dev --no-emit-project --format requirements.txt -o orchestration/requirements-nfz.txt
cd orchestration && astro dev restart
```

## Scope

This is the local orchestration environment. The daily production run is GitHub Actions (planned);
Airflow on a server comes later.
