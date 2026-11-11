{# One venue's exported raw file: when each message was received, and the message. #}
{% macro raw_messages(venue) %}
  read_json(
    '{{ var("raw_dir") }}/{{ venue }}.jsonl',
    format = 'newline_delimited',
    columns = {recv_ms: 'BIGINT', msg: 'JSON'}
  )
{% endmacro %}

{# '2026-11-11T12:41:47.632107Z' as a UTC timestamp with microseconds. #}
{% macro iso_utc(expression) %}
  cast(replace(replace({{ expression }}, 'T', ' '), 'Z', '') as timestamp)
{% endmacro %}

{# The hour being reconciled, as a timestamp. #}
{% macro hour_start() %}
  cast('{{ var("hour_start") }}' as timestamp)
{% endmacro %}
