FROM astrocrpublic.azurecr.io/runtime:3.3-12

# The pipeline (dlt + dbt) gets its own virtualenv, pinned from ../uv.lock, so it never
# fights with Airflow's own dependencies. Regenerate after changing ../pyproject.toml:
#   uv export --frozen --no-dev --no-emit-project --format requirements.txt -o orchestration/requirements-nfz.txt
COPY requirements-nfz.txt /tmp/requirements-nfz.txt
RUN python -m venv /usr/local/airflow/nfz_venv \
    && /usr/local/airflow/nfz_venv/bin/pip install --no-cache-dir --require-hashes -r /tmp/requirements-nfz.txt
