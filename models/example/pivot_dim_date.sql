select 
month_name,
{{ dbt_utils.pivot('year_actual', dbt_utils.get_column_values(ref('dim_date'), 'year_actual', order_by='year_actual', where="year_actual >= extract(year from now()) - 5")) }}
from {{ ref('dim_date') }}
group by 1