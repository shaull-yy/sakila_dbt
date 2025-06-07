{{
	config(
		   uniqu_key = 'customer_id',
		   post_hook = macro_z_refresh_from(this)
		  )
}}  

with refresh_date as (
	select from_date
	from {{ source('stg', 'z_refresh_from') }}
	where   table_name = '{{this}}'
		and to_refresh = 1
)
select 
	{{macro_concat('c.first_name', 'c.last_name') }} as cust_name  -- c.first_name || ' ' || c.last_name as cust_name
	,split_part(email, '@', 2) AS domain_name
	,ci.city
	,case 
		when c.active = 1 then 'yes'
		else 'no'
	end as active_ind
	,c.*
from {{ source('stg', 'cust') }} as c
	left join {{ source('stg', 'address') }} as a  on c.address_id = a.address_id
	left join {{ source('stg', 'city') }} as ci    on a.city_id  = ci.city_id
{% if is_incremental() %}
  where c.last_update > coalesce(
	 (select from_date from refresh_date)
	,(select max(last_update)  from {{ this }})
	, '1900-01-01')
{% endif %}