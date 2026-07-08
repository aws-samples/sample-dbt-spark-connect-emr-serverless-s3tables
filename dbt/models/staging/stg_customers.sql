with source as (
    select * from {{ ref('raw_customers') }}
),

renamed as (
    select
        cast(id as bigint)      as customer_id,
        first_name,
        last_name
    from source
)

select * from renamed
