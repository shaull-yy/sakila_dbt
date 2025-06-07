

select
	f.*
	,c."name" as category_name
	,l."name" as language
FROM {{ source('stg', 'film') }}    as f
left join {{ source('stg', 'film_cat') }} as fc   on f.film_id = fc.film_id
left join {{ source('stg', 'cat') }}      as c    on fc.category_id  = c.category_id
left join {{ source('stg', 'lang') }}     as l    on f.language_id  = l.language_id
