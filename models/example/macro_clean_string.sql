select
	f.film_id
	,f.title
	,f.description
	,{{macro_clean_string('f.description')}} as clean_desc
from {{ source('stg', 'film') }} as f