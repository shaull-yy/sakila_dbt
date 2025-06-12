{{
	config(
		   uniqu_key = 'customer_id',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{log_model('end')}}"],
		   indexes= [{'columns': ['create_date']}]
		  )
}}  

select 
	{{dbt_utils.star(from=source('stg', 'cust'), except=["activebool"], relation_alias = 'c') }}
	,{{macro_concat('c.first_name', 'c.last_name') }} as cust_name  -- c.first_name || ' ' || c.last_name as cust_name
	,split_part(email, '@', 2) AS domain_name
	,loc.city
	,case 
		when c.active = 1 then 'yes'
		else 'no'
	end as active_ind
	,loc.country
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
from {{ source('stg', 'cust') }} as c
left join {{ ref('dim_location') }} as loc   on c.address_id = loc.address_id