{% macro z_refresh_from(col_a, col_b) %}
  select from_date from z_refresh_from where to_refresh = 1 and table_name = {{this}}
{% endmacro %}