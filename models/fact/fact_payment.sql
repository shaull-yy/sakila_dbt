
{{
  config(
	unique_key = 'payment_id',
	pre_hook = "{{log_model('start')}}",
	post_hook = ["{{manual_refresh(this)}}", "{{log_model('end')}}"]
	)
}}

with refresh_date as (
	select from_date
	from {{ source('stg', 'z_refresh_from') }}
	where   table_name = '{{this}}'
		and to_refresh = 1
)
SELECT
	p.*
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
FROM {{ source('stg', 'payment') }}    as p
{% if is_incremental() %}
  where p.payment_date >= coalesce(
		(select from_date from refresh_date),
		 (select max(payment_date) from {{ this }}), 
		 '1900-01-01')
{% endif %}