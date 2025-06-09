{{
	config(
		   uniqu_key = 'store_id',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{manual_refresh(this)}}", "{{log_model('end')}}"]
		  )
}}  

select 
	str.*
	,stf.first_name as mang_first_name
	,stf.last_name  as mang_last_name
	,cit.city
	,cnt.country
	,coalesce(stf.staff_id, -1) staff_staff_id
	,coalesce(add.address_id, -1) address_add_id
	,coalesce(cit.city_id, -1) city_city_id
	,coalesce(cnt.country_id, -1) country_country_id
from {{ source('stg', 'store') }} str 
left join {{ source('stg', 'staff') }}   as stf  on str.manager_staff_id = stf.staff_id 
left join {{ source('stg', 'address') }} as add  on str.address_id  =  add.address_id
left join {{ source('stg', 'city') }}    as cit  on add.city_id = cit.city_id
left join {{ source('stg', 'country') }} as cnt  on cit.country_id  = cnt.country_id 


