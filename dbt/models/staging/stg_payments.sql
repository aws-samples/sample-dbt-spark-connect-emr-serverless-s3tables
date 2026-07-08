with source as (
    select * from {{ ref('raw_payments') }}
),

renamed as (
    select
        cast(id as bigint)        as payment_id,
        cast(order_id as bigint)  as order_id,
        payment_method,
        -- amounts are stored in cents in the raw feed
        cast(amount as double) / 100.0 as amount
    from source
)

select * from renamed
