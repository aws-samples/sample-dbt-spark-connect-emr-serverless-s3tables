-- Override dbt-spark's drop_relation for Amazon S3 Tables.
--
-- S3 Tables rejects DROP TABLE without purge:
--   "S3 Tables does not support the dropTable operation with purge=false"
-- dbt's default `drop table if exists` sends purge=false, which breaks any re-run
-- that has to replace an existing table (seeds full-refresh, table materialization,
-- etc.). Emitting PURGE makes Spark call the catalog's purge path, which S3 Tables
-- supports. Views don't accept PURGE, so only add it for tables.
{% macro spark__drop_relation(relation) -%}
  {%- if relation.type == 'table' -%}
    {% call statement('drop_relation', auto_begin=False) -%}
      drop table if exists {{ relation }} purge
    {%- endcall %}
  {%- else -%}
    {% call statement('drop_relation', auto_begin=False) -%}
      drop {{ relation.type }} if exists {{ relation }}
    {%- endcall %}
  {%- endif -%}
{%- endmacro %}
