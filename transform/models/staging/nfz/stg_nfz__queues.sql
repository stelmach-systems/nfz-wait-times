-- One row per queue per case per snapshot day, typed and renamed; `pcus` parsed per ADR-04.
--
-- `pcus` is NFZ's forecast of the wait ("3 mies. 1 tydz.", "6 dni"). Formats seen on 2026-11-12:
-- "N dni", "N dzień", "N mies.", "N mies. N tydz.", "N mies. N tyg.", plus "-" and NULL.
-- Assumptions (ADR-04, to confirm with NFZ docs):
--   * a month is 28..31 days, so it widens the range instead of pretending to be 30;
--   * when no day count is shown ("N mies.", "N mies. N tydz.") the days were truncated,
--     so `days_max` adds up to 6 days.

with source as (

    select * from {{ source('raw_nfz', 'queues') }}

),

pcus_parts as (

    select
        *,
        regexp_full_match(
            dates__pcus,
            '(?:(\d+) mies\.)?\s*(?:(\d+) (?:tydz\.|tyg\.))?\s*(?:(\d+) (?:dni|dzień))?'
        ) and trim(dates__pcus) <> '' as pcus_is_known_format,
        try_cast(nullif(regexp_extract(dates__pcus, '(\d+) mies\.', 1), '') as integer) as pcus_months,
        try_cast(nullif(regexp_extract(dates__pcus, '(\d+) (?:tydz\.|tyg\.)', 1), '') as integer) as pcus_weeks,
        try_cast(nullif(regexp_extract(dates__pcus, '(\d+) (?:dni|dzień)', 1), '') as integer) as pcus_days

    from source

),

parsed as (

    select
        *,
        case
            when dates__applicable is null and dates__pcus is null then 'no_data'
            when dates__applicable = false and dates__pcus = '-' then 'not_applicable'
            when dates__applicable = true and pcus_is_known_format then 'forecast'
            else 'unexpected'
        end as pcus_status

    from pcus_parts

),

renamed as (

    select
        -- keys
        id || '|' || "case" || '|' || snapshot_date as queue_snapshot_key,
        id as queue_id,
        "case" as case_type,  -- 1 = stable, 2 = urgent
        snapshot_date,

        -- benefit and provider
        benefit,
        provider_code,
        registry_letter,  -- W = healthcare entity, L = individual practice; NULL treated like L upstream
        id_resort_part_vii as resort_code_part_vii,
        id_resort_part_viii as resort_code_part_viii,
        {{ yes_no_to_boolean('benefits_for_children') }} as is_for_children,
        age_range,
        {{ yes_no_to_boolean('many_places') }} as has_many_places,
        {{ yes_no_to_boolean('queue_in_cer') }} as is_in_central_register,

        -- location (address and coordinates only for W, ADR-02)
        locality,
        county,
        teryt_place,
        teryt_provider,
        address,
        latitude,
        longitude,

        -- wait forecast
        try_cast(dates__date_situation_as_at as date) as situation_as_at_date,
        dates__applicable as is_forecast_applicable,
        dates__pcus as pcus_raw,
        pcus_status,
        case when pcus_status = 'forecast'
            then coalesce(pcus_months, 0) * 28 + coalesce(pcus_weeks, 0) * 7 + coalesce(pcus_days, 0)
        end as pcus_days_min,
        case when pcus_status = 'forecast'
            then coalesce(pcus_months, 0) * 31 + coalesce(pcus_weeks, 0) * 7 + coalesce(pcus_days, 0)
                + case when pcus_days is null then 6 else 0 end
        end as pcus_days_max,

        -- monthly statistics reported by the provider
        statistics__provider_data__awaiting as stats_awaiting,
        statistics__provider_data__removed as stats_removed,
        statistics__provider_data__average_period as stats_average_period_days,
        statistics__provider_data__update as stats_period,  -- 'YYYY-MM'

        -- accessibility (Y/N; empty string = not reported -> NULL)
        {{ yes_no_to_boolean('elevator') }} as has_elevator,
        {{ yes_no_to_boolean('ramp') }} as has_ramp,
        {{ yes_no_to_boolean('toilet') }} as has_accessible_toilet,
        {{ yes_no_to_boolean('wheelchairs') }} as has_wheelchairs,
        {{ yes_no_to_boolean('car_park') }} as has_car_park,
        {{ yes_no_to_boolean('corridors') }} as has_accessible_corridors,
        {{ yes_no_to_boolean('automatic_door') }} as has_automatic_door,
        {{ yes_no_to_boolean('ac') }} as has_air_conditioning,
        {{ yes_no_to_boolean('anesthesia') }} as offers_anesthesia,
        type_building as building_type,
        public_transport_lines,

        -- lineage
        _dlt_load_id as dlt_load_id

    from parsed

),

final as (

    select
        *,
        case
            when pcus_days_min is null then null
            when pcus_days_min = 0 then 0
            when pcus_days_min <= 7 then 1
            when pcus_days_min <= 30 then 2
            when pcus_days_min <= 90 then 3
            when pcus_days_min <= 180 then 4
            else 5
        end as pcus_bucket_order,
        case
            when pcus_days_min is null then null
            when pcus_days_min = 0 then '0 dni'
            when pcus_days_min <= 7 then 'do tygodnia'
            when pcus_days_min <= 30 then 'do miesiąca'
            when pcus_days_min <= 90 then '1–3 mies.'
            when pcus_days_min <= 180 then '3–6 mies.'
            else 'ponad 6 mies.'
        end as pcus_bucket

    from renamed

)

select * from final
