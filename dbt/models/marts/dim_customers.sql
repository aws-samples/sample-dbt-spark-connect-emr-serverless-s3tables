-- Customer dimension: one row per customer with lifetime order/payment stats.
-- Materialized as an Iceberg table in the S3 Tables catalog.

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
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

customer_orders as (
    select
        c.customer_id,
        min(o.order_date)                    as first_order_date,
        max(o.order_date)                    as most_recent_order_date,
        count(o.order_id)                    as number_of_orders,
        coalesce(sum(op.amount), 0)          as lifetime_value
    from customers c
    left join orders o
        on c.customer_id = o.customer_id
    left join order_payments op
        on o.order_id = op.order_id
    group by c.customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    co.first_order_date,
    co.most_recent_order_date,
    co.number_of_orders,
    co.lifetime_value
from customers c
left join customer_orders co
    on c.customer_id = co.customer_id
