
{{
  config(
	unique_key = 'rental_id',
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
	r.*
	,to_char(r.rental_date, 'YYYYMMDD')::integer as date_key
	,round(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600.0, 1) as rent_duration_hr
	,case when r.return_date is null then 0
		else 1
	end as is_return
	,i.film_id
	,i.store_id
	,{{coalesces_id('i', 'inventory_id', 'inventory')}}
	,{{coalesces_id('cust', 'customer_id', 'cust')}}
	,{{coalesces_id('s', 'store_id', 'store')}}
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
FROM {{ source('stg', 'rental') }}          as r
left join {{ source('stg', 'inventory') }}  as i   on i.inventory_id = r.inventory_id 
left join {{ source('stg', 'cust') }}       as cust on r.customer_id = cust.customer_id
left join {{ source('stg', 'store') }}      as s    on i.store_id = s.store_id
{% if is_incremental() %}
  where r.last_update >= coalesce(
		(select from_date from refresh_date),
		(select max(last_update) from {{ this }}), 
		'1900-01-01')
{% endif %}