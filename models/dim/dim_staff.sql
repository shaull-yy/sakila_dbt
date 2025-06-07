

select 
stf.*
,str.address_id as store_address_id
,str.manager_staff_id
from {{ source('stg', 'staff') }} stf 
left join {{ source('stg', 'store') }} str  on stf.store_id  = str.store_id

