{{
	config(
		   uniqu_key = 'customer_id',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{log_model('end')}}", "{{manual_refresh(this)}}"],
		   indexes= [{'columns': ['last_update']}]
		  )
}} 

with max_rental_date as (
{% if is_incremental() %}
	select 
		max(last_update) as max_last_update
	from {{this}}
{% else %}
	select
		null as max_last_update
	{% endif %}
),
refresh_date as (
	select from_date
	from {{ source('stg', 'z_refresh_from') }}
	where   table_name = '{{this}}'
		and to_refresh = 1
),
rental_cust as (
select
	r.customer_id
	, min(r.rental_date::date) as created_date
	, max(r.rental_date::date) as last_update 
from sk_stg.rental as r
group by r.customer_id
)
select 
	rc.customer_id
	,rc.created_date
	,rc.last_update
from rental_cust as rc
left join max_rental_date as max_rental_date on 1 = 1
left join refresh_date    as refresh_date    on 1 = 1
where 1 = 1
{% if is_incremental() %}
	and
	rc.last_update >= coalesce(refresh_date.from_date, max_rental_date.max_last_update, '{{ var('init_date') }}')
{% endif %} 
