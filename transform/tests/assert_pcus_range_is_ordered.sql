-- A parsed forecast must have both bounds and min <= max.
select queue_snapshot_key, pcus_raw, pcus_days_min, pcus_days_max
from {{ ref('stg_nfz__queues') }}
where pcus_status = 'forecast'
  and (pcus_days_min is null or pcus_days_max is null or pcus_days_min > pcus_days_max)
