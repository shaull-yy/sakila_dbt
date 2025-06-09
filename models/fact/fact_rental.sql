{{
  config(
	unique_key = 'rental_id',
	post_hook = "{{manual_refresh(this)}}"
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
	,p.payment_id 
	,p.staff_id as pay_stuff_id
	,p.amount
	,p.payment_date
FROM {{ source('stg', 'rental') }}          as r
left join {{ source('stg', 'inventory') }}  as i   on i.inventory_id = r.inventory_id 
left join {{ source('stg', 'payment') }}    as p   on r.rental_id  = p.rental_id 
{% if is_incremental() %}
  where r.last_update >= coalesce(
		(select from_date from refresh_date),
		 select max(last_update) from {{ this }}), 
		 '1900-01-01')
{% endif %}