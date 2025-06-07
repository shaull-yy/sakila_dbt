{% macro manual_refresh(tb_name) %}
  update {{source('stg', 'z_refresh_from')}} set to_refresh = 0 where table_name = '{{tb_name}}'
{% endmacro %}