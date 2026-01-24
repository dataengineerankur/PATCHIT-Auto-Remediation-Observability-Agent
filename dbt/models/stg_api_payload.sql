{{ config(schema='analytics') }}

-- Intentionally brittle model for MVP failure reproduction:
-- - dbt_missing_column: `amount` missing in raw.api_payload -> binder error
-- - schema drift/type drift: casting can fail

select
  run_id,
  event_time,
  user_id,
  cast(amount as double) as amount
from raw.api_payload



