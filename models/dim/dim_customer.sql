{{
	config(
		   uniqu_key = 'customer_id',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{manual_refresh(this)}}", "{{log_model('end')}}"],
		   indexes= [{'columns': ['create_date']}],
		   identifier = 'cust2'
		  )
}}  

with refresh_date as (
	select from_date
	from {{ source('stg', 'z_refresh_from') }}
	where   table_name = '{{this}}'
		and to_refresh = 1
)
select 
	{{dbt_utils.star(from=source('stg', 'cust'), except=['"activebool"'], relation_alias = 'c') }}
	,{{macro_concat('c.first_name', 'c.last_name') }} as cust_name  -- c.first_name || ' ' || c.last_name as cust_name
	,split_part(email, '@', 2) AS domain_name
	,ci.city
	,case 
		when c.active = 1 then 'yes'
		else 'no'
	end as active_ind
	,co.country_id
	,co.country
	,{{coalesces_id('a', 'address_id', 'address')}}
	,{{coalesces_id('ci', 'city_id', 'city')}}
	,{{coalesces_id('co', 'country_id', 'country')}}
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
from {{ source('stg', 'cust') }} as c
	left join {{ source('stg', 'address') }} as a   on c.address_id = a.address_id
	left join {{ source('stg', 'city') }}    as ci  on a.city_id  = ci.city_id
	left join {{ source('stg', 'country') }} as co  on ci.country_id = co.country_id
{% if is_incremental() %}
  where c.last_update > coalesce(
	 (select from_date from refresh_date)
	,(select max(last_update)  from {{ this }})
	, '1900-01-01')
{% endif %}