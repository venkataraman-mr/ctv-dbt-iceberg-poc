{#
  Spark -> Trino SQL compatibility helpers (generic; reusable across models).

  substring_index(str, delim, count) — Spark's SUBSTRING_INDEX, which Trino has no builtin for.
    Spark semantics:
      count > 0 : substring BEFORE the count-th delimiter, counting from the LEFT
                  (i.e. the first `count` segments joined by delim).
      count < 0 : substring AFTER the |count|-th delimiter, counting from the RIGHT
                  (i.e. the last |count| segments joined by delim).
    If delim is absent, returns str unchanged (Trino split returns [str] -> slice/join give str).
    Built from split() (literal delimiter, like Spark SUBSTRING_INDEX) + slice() + array_join().
#}
{% macro substring_index(str, delim, count) %}
  {%- set d = "'" ~ delim ~ "'" -%}
  {%- if count >= 0 -%}
    array_join(slice(split({{ str }}, {{ d }}), 1, {{ count }}), {{ d }})
  {%- else -%}
    array_join(slice(split({{ str }}, {{ d }}), {{ count }}, {{ (count | abs) }}), {{ d }})
  {%- endif -%}
{% endmacro %}


{#
  json_object_str(pairs) — build a JSON object string from a list of (key, value_sql_expr) pairs.

  Trino has no json_object() (that's Postgres), and cast(row(...) as json) drops field names, so we
  build the object as a map of VARCHAR values: cast(map(array[keys], array[vals]) as json), json_format'd
  to text. Every value is cast to varchar, so the stored JSON has string-typed values
  (e.g. {"market_id":"302"}). That is FUNCTIONALLY equivalent to the legacy Spark to_json(struct(...))
  for our consumers: the Postgres proc reads keys with `->>` (text) then casts (::integer, ::timestamp),
  which works identically on "302" or 302. Timestamp values must be pre-formatted as ::timestamp-castable
  strings by the caller (e.g. date_format(cast(ts as timestamp(6)), '%Y-%m-%d %H:%i:%s.%f')).

  `pairs` is a list of [key_string, value_sql_expr_string].
#}
{% macro json_object_str(pairs) %}
  {%- set keys = [] -%}
  {%- set vals = [] -%}
  {%- for pair in pairs -%}
    {%- do keys.append("'" ~ pair[0] ~ "'") -%}
    {%- do vals.append("cast(" ~ pair[1] ~ " as varchar)") -%}
  {%- endfor -%}
  json_format(cast(map(array[{{ keys | join(', ') }}], array[{{ vals | join(', ') }}]) as json))
{% endmacro %}
