{{
  config(
	unique_key = 'rental_id'
	)
}}

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
  where r.last_update >= coalesce((select max(last_update) from {{ this }}), '1900-01-01')
{% endif %}