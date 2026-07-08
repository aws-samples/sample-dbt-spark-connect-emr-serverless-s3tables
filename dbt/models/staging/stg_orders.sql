with source as (
    select * from {{ ref('raw_orders') }}
),

renamed as (
    select
        cast(id as bigint)          as order_id,
        cast(user_id as bigint)     as customer_id,
        cast(order_date as date)    as order_date,
        status
    from source
)

select * from renamed
