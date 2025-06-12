
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
),
null_cust as (
	select rental_id
	from {{this}}
	where customer_id = -1
),
base_rental as (
	SELECT
	r.rental_id
	,r.rental_date
	,r.inventory_id
	,cust.customer_id as customer_id_tmp
	,r.return_date
	,r.staff_id
	,r.last_update
	,i.film_id
	,i.store_id
FROM {{ source('stg', 'rental') }}          as r
left join {{ source('stg', 'inventory') }}  as i   on i.inventory_id = r.inventory_id 
left join {{ source('stg', 'cust') }}       as cust on r.customer_id = cust.customer_id
left join {{ source('stg', 'store') }}      as s    on i.store_id = s.store_id
),
base_rental2 as (
	select
	base_rental.*
	from base_rental as base_rental
{% if is_incremental() %}
  where base_rental.last_update > coalesce(
		(select from_date from refresh_date),
		(select max(last_update) from {{ this }}), 
		'1900-01-01')
  union
	select
	base_rental.*
	from base_rental as base_rental
	inner join null_cust as null_cust on null_cust.rental_id = base_rental.rental_id
{% endif %}
)
SELECT
	base_rent.rental_id
	,base_rent.rental_date
	,base_rent.inventory_id
	,base_rent.return_date
	,base_rent.staff_id
	,base_rent.last_update
	,base_rent.film_id
	,base_rent.store_id
	,case when base_rent.customer_id_tmp is not null then base_rent.customer_id_tmp else -1 end as customer_id
	,to_char(base_rent.rental_date, 'YYYYMMDD')::integer as date_key
	,round(EXTRACT(EPOCH FROM (base_rent.return_date - base_rent.rental_date)) / 3600.0, 1) as rent_duration_hr
	,case when base_rent.return_date is null then 0 else 1 end as is_return
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
FROM base_rental2   as base_rent
