{{
  config(
	materialized = 'ephemeral'
	)
}}

select 
	add.address_id
	,add.address
	,add.district
	,city.city 
	,count.country
from {{ source('stg', 'address') }}        as add
left join  {{ source('stg', 'city') }}     as city   on add.city_id = city.city_id
left join  {{ source('stg', 'country') }}  as count  on city.country_id = count.country_id