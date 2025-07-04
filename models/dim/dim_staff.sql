{{
	config(
		   uniqu_key = 'staff_id',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{log_model('end')}}", "insert into {{ this }} (staff_id, first_name, last_name) values (-1,'NA', 'NA')"]
		  )
}}  

select 
	stf.staff_id, stf.first_name, stf.last_name, stf.email, stf.last_update
	,case when stf.active = true then 1
		else 0
	end as active_int
	,case when stf.active = true then 'yes'
		else 'no'
	end as active_desc
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
from {{ source('stg', 'staff') }} stf 

