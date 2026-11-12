-- ADR-02: address and coordinates may only be present for healthcare entities (W).
-- Individual practices (L) and records without a letter point at a person. Second line of defence after dlt.
select queue_snapshot_key, registry_letter
from {{ ref('stg_nfz__queues') }}
where registry_letter is distinct from 'W'
  and (address is not null or latitude is not null or longitude is not null)
