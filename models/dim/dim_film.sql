
{{
	config(
		   uniqu_key = 'film_id',
		   pre_hook = "{{log_model('start')}}",
		   post_hook = ["{{log_model('end')}}", "insert into {{ this }} (film_id, title, release_year) values (-1,'NA', 1990)"]
		  )
}} 

{% set my_column_list = ['trailers', 'deleted scenes', 'behind the scenes', 'commentaries'] %}

select
	f.*
	,case when f.length <= 75 then 'short'
		when f.length <= 120 then 'medium'
		else 'long'
	end as length_desc
	,c."name" as category_name
	,l."name" as "language"
	{% for feature in my_column_list %}
	,case when special_features  ilike '%{{ feature }}%' then 1 else 0 end "is_{{feature}}"
	{% endfor %}
	,'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time_utc
FROM {{ source('stg', 'film') }}    as f
left join {{ source('stg', 'film_cat') }} as fc   on f.film_id = fc.film_id
left join {{ source('stg', 'cat') }}      as c    on fc.category_id  = c.category_id
left join {{ source('stg', 'lang') }}     as l    on f.language_id  = l.language_id
