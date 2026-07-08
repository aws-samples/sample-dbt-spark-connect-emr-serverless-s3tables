-- Order fact table. Demonstrates an incremental model that MERGEs into an Iceberg
-- table (Iceberg + Spark 4 support row-level MERGE). On an incremental run only
-- orders newer than the current max order_date are processed and merged on order_id.
--
-- Iceberg hidden partitioning via a partition transform: dbt passes the `partition_by`
-- value straight into PARTITIONED BY (...), so transforms like days()/months()/years()/
-- bucket(N,col)/truncate(N,col) work. Pick a granularity that matches data volume
-- (months/years for sparse data).
--
-- Per-column Iceberg write metrics via `tblproperties` (write.metadata.metrics.*):
-- these control the lower/upper bounds and value/null counts stored in manifests and
-- used for scan pruning / predicate pushdown. Default "counts" for every column, with
-- "full" stats collected for the `amount` and `status` columns.
--
-- MERGE partition pruning via `incremental_predicates`: dbt-spark ANDs these straight
-- into the merge ON clause next to the unique_key match, so the generated statement is
--   merge ... on DBT_INTERNAL_DEST.order_date >= date_add(current_date(), -N)
--               and DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
-- Because the table is partitioned by days(order_date), the date bound lets Iceberg
-- prune to just the recent day-partitions instead of scanning the whole target to find
-- order_id matches -- the key perf lever on large Iceberg tables.
--
-- TRADEOFF (important): a predicate in ON also gates matching. A source row whose
-- order_id already exists in the target but in a partition OUTSIDE the window will not
-- match, so it is inserted as a duplicate. The window must cover every partition where
-- an update can land. It is safe here because this model only ever processes orders
-- strictly newer than the current max order_date, and the `unique` test on order_id is
-- a backstop. Tune the window with `--vars '{merge_lookback_days: 30}'`.
--
-- NOTE: keep these notes OUTSIDE the config() block, and do not write Jinja
-- expression braces in comments -- dbt renders Jinja everywhere in the file
-- (comments included), so an empty config call in a comment would be executed.

{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="order_id",
        file_format="iceberg",
        partition_by="days(order_date)",
        incremental_predicates=[
            "DBT_INTERNAL_DEST.order_date >= date_add(current_date(), -" ~ var("merge_lookback_days", 3) ~ ")"
        ],
        tblproperties={
            "write.metadata.metrics.default": "counts",
            "write.metadata.metrics.column.amount": "full",
            "write.metadata.metrics.column.status": "full"
        }
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_payments as (
    select
        order_id,
        sum(amount) as amount
    from payments
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        coalesce(op.amount, 0) as amount
    from orders o
    left join order_payments op
        on o.order_id = op.order_id
)

select * from final

{% if is_incremental() %}
where order_date > (select coalesce(max(order_date), date '1900-01-01') from {{ this }})
{% endif %}
