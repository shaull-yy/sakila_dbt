
{{
  config(
	unique_key = 'staff_id'
	)
}}

select 
stf.*
,str.address_id as store_address_id
,str.manager_staff_id
from {{ source('stg', 'staff') }} stf 
left join {{ source('stg', 'store') }} str  on stf.store_id  = str.store_id

{% if is_incremental() %}
  and stf.last_update >= coalesce((select max(last_update) from {{ this }}), '1900-01-01')
{% endif %}

