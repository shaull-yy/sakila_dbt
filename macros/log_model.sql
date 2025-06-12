{% macro log_model(start_end) %}

{% if start_end == 'start' %}
  insert into dbt_log (id, model, start_date, end_date, run_duration, run_dur_sec, etl_date_utc) values('{{ invocation_id}}', '{{this}}', now(), null, null, null, '{{ run_started_at }}'::timestamp);
{%else %}
  update dbt_log set end_date = now(), run_duration = now() - start_date, run_dur_sec = EXTRACT(EPOCH FROM (now() - start_date)) where id = '{{ invocation_id}}' and model = '{{this}}';
{% endif %}

{% endmacro %}