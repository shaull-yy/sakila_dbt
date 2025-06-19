{{
	config(
		   uniqu_key = 'store_id',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{log_model('end')}}", "insert into {{ this }} (store_id, city, country) values (-1,'NA', 'NA')"]
		  )
}}  

select 
	str.*
	,stf.first_name as mang_first_name
	,stf.last_name  as mang_last_name
	,loc.city
	,loc.country
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
from {{ source('stg', 'store') }} str 
left join {{ source('stg', 'staff') }}   as stf  on str.manager_staff_id = stf.staff_id 
left join {{ ref('dim_location') }}      as loc  on str.address_id = loc.address_id


