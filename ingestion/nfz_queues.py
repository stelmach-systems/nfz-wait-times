"""NFZ "Terminy Leczenia" API v1.4 -> DuckDB (raw_nfz.queues).

Reads every queue in pomorskie for both cases, strips personal data before
anything leaves the generator (ADR-02) and stores one snapshot per day.

    uv run python ingestion/nfz_queues.py                    # local DuckDB (dbt target dev)
    uv run python ingestion/nfz_queues.py --target prod      # MotherDuck, token from MOTHERDUCK_TOKEN
    uv run python ingestion/nfz_queues.py --skip-if-loaded   # scheduled runs: keep a day already loaded
"""

import argparse
import logging
import os
import re
import time
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import dlt
import duckdb
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

# --- Source configuration -------------------------------------------------
BASE_URL = "https://apinfz.nfz.gov.pl/app-itl-api-pcus/"  # v1.4; the old api.nfz.gov.pl/app-itl-api/ (v1.3) has empty `dates`
API_VERSION = "1.4"
USER_AGENT = "nfz-wait-times (+https://github.com/stelmach-systems/nfz-wait-times)"

PROVINCE = "11"  # pomorskie
CASES = (1, 2)  # 1 = stable, 2 = urgent; separate queues with different ids
PAGE_LIMIT = 25  # API maximum; anything above returns HTTP 400
SECONDS_BETWEEN_REQUESTS = 1.0  # be polite: at most one request per second

# --- Destination ------------------------------------------------------------
DUCKDB_PATH = Path(__file__).resolve().parents[1] / "data" / "nfz.duckdb"  # same file as dbt target `dev`
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE") or "nfz_wait_times"  # same default as dbt target `prod`
DATASET = "raw_nfz"
TIMEZONE = ZoneInfo("Europe/Warsaw")  # the snapshot day is a Polish calendar day, also on UTC CI runners

# --- Personal data (ADR-02, variant c: strip at extraction) ---------------
# Allowlist: only these attributes are kept. A field NFZ adds in the future is
# dropped (and reported) until someone decides it is safe.
KEEP = {
    "case", "benefit", "provider-code", "queue-in-cer",
    "age-range", "benefits-for-children", "benefits-provided", "many-places",
    "locality", "county", "teryt-place", "teryt-provider",
    "dates", "statistics",
    "ac", "anesthesia", "automatic-door", "car-park", "corridors", "covid-19", "elevator",
    "public-transport-lines", "ramp", "toilet", "type-building", "wheelchairs",
    "id-resort-part-VII", "id-resort-part-VIII",
}
# Location of a healthcare entity (W) is public business data; for an individual
# practice (L) it often points at a person. Kept only when the letter is surely W.
W_ONLY = {"address", "latitude", "longitude"}
# Known personal or contact data: never stored. `registry-number` is reduced to its letter.
# `place` is free text typed by the provider and carries doctors' names for both L and W.
DROP = {
    "provider", "phone", "information-phone", "registration-phone", "additional-phone",
    "fax", "email", "website", "nip-provider", "regon-provider", "registry-number", "place",
}
REGISTRY_LETTER = re.compile(r"-([A-Z])-")

log = logging.getLogger(__name__)
_reported_unknown: set[str] = set()


class NfzLinkPaginator(JSONLinkPaginator):
    """Follows `links.next`, fixing two quirks of the NFZ API.

    The link is root-relative (`/queues?page=2...`), so a plain urljoin drops the
    `/app-itl-api-pcus/` prefix, and it omits `api-version`. We rebuild it against
    BASE_URL and pin the version explicitly instead of relying on the server default.
    """

    def __init__(self) -> None:
        super().__init__(next_url_path="links.next")

    def update_request(self, request) -> None:
        if self._next_reference and self._next_reference.startswith("/"):
            url = urljoin(BASE_URL, self._next_reference.lstrip("/"))
            if "api-version=" not in url:
                url += f"&api-version={API_VERSION}"
            self._next_reference = url
        super().update_request(request)


def make_client() -> RESTClient:
    # The default dlt session already retries on connection errors, timeouts, 429 and 5xx.
    return RESTClient(base_url=BASE_URL, headers={"User-Agent": USER_AGENT})


def query_params(case: int) -> dict:
    return {
        "case": case,
        "province": PROVINCE,
        "limit": PAGE_LIMIT,
        "format": "json",
        "api-version": API_VERSION,
    }


def registry_letter(registry_number: str | None) -> str | None:
    match = REGISTRY_LETTER.search(registry_number or "")
    return match[1] if match else None


def scrub(record: dict) -> dict:
    """Flattens one API record and removes personal data (ADR-02 variant c)."""
    attrs = record["attributes"]
    letter = registry_letter(attrs.get("registry-number"))
    row = {"id": record["id"], "registry-letter": letter}
    row |= {key: attrs.get(key) for key in KEEP}
    row |= {key: attrs.get(key) if letter == "W" else None for key in W_ONLY}

    unknown = attrs.keys() - KEEP - W_ONLY - DROP - _reported_unknown
    if unknown:
        log.warning("NFZ API returned new attributes, dropped until reviewed: %s", sorted(unknown))
        _reported_unknown.update(unknown)
    return row


def fetch_queues(client: RESTClient, case: int) -> Iterator[list[dict]]:
    """Yields one list of scrubbed queue records per API page and checks completeness at the end."""
    expected = None
    seen_ids: set[str] = set()
    for page in client.paginate(
        "queues",
        params=query_params(case),
        paginator=NfzLinkPaginator(),
        data_selector="data",
    ):
        if expected is None:
            expected = page.response.json()["meta"]["count"]
        seen_ids.update(record["id"] for record in page)
        yield [scrub(record) for record in page]
        time.sleep(SECONDS_BETWEEN_REQUESTS)

    # The list can be republished mid-read; then pages shift and records get skipped or repeated.
    if len(seen_ids) != expected:
        raise RuntimeError(f"case={case}: got {len(seen_ids)} unique ids, API reported {expected}")


# One row per queue per day. Re-running on the same day replaces that day
# (merge_key), earlier days are never touched, so the table is the history.
@dlt.resource(
    name="queues",
    primary_key=("id", "case", "snapshot_date"),
    merge_key="snapshot_date",
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
)
def queues(snapshot_date: date) -> Iterator[list[dict]]:
    client = make_client()
    for case in CASES:
        for records in fetch_queues(client, case):
            yield [record | {"snapshot_date": snapshot_date} for record in records]


def destination(target: str):
    if target == "prod":
        token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
        # Fail before the ~7-minute extract, not at load time.
        if not token:
            raise SystemExit("MOTHERDUCK_TOKEN is not set")
        # The token comes from the environment only and is handed to dlt in memory (never a file or a URL here).
        return dlt.destinations.motherduck(credentials={"database": MOTHERDUCK_DATABASE, "password": token})
    return dlt.destinations.duckdb(str(DUCKDB_PATH))


def already_loaded(snapshot_date: date, attempts: int = 3, wait_seconds: int = 60) -> bool:
    """True when the local DuckDB already holds this day (used by the scheduled local runs).

    DuckDB allows one writing process per file. If someone holds it open (a DuckDB shell, a dbt run),
    wait and retry; never guess False, because that would start a second writer.
    """
    if not DUCKDB_PATH.exists():
        return False
    for attempt in range(1, attempts + 1):
        try:
            with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
                exists = con.sql(
                    "select count(*) from information_schema.tables where table_schema = ? and table_name = 'queues'",
                    params=[DATASET],
                ).fetchone()[0]
                if not exists:
                    return False
                sql = f"select count(*) from {DATASET}.queues where snapshot_date = ?"
                return con.sql(sql, params=[snapshot_date]).fetchone()[0] > 0
        except duckdb.IOException as error:
            if attempt == attempts:
                raise SystemExit(f"{DUCKDB_PATH} is locked by another process: {error}")
            log.warning("DuckDB file locked, retrying in %s s (%s/%s)", wait_seconds, attempt, attempts)
            time.sleep(wait_seconds)
    return False


def run(snapshot_date: date | None = None, target: str = "dev", skip_if_loaded: bool = False) -> None:
    snapshot_date = snapshot_date or datetime.now(TIMEZONE).date()
    if skip_if_loaded:
        if target != "dev":
            raise SystemExit("--skip-if-loaded checks the local DuckDB only (target dev)")
        if already_loaded(snapshot_date):
            print(f"snapshot_date={snapshot_date} already loaded, skipping")
            return
    pipeline = dlt.pipeline(
        # Separate local state per destination, so dev and prod runs never share a working directory.
        pipeline_name="nfz_queues" if target == "dev" else "nfz_queues_motherduck",
        destination=destination(target),
        dataset_name=DATASET,
    )
    load_info = pipeline.run(queues(snapshot_date))
    print(f"snapshot_date={snapshot_date} target={target}")
    print(load_info)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=("dev", "prod"), default="dev")
    parser.add_argument("--skip-if-loaded", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    run(target=args.target, skip_if_loaded=args.skip_if_loaded)
