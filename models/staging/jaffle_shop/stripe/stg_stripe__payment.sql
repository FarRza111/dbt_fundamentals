-- select
--     id as payment_id,
--     orderid as order_id,
--     paymentmethod as payment_method,
--     status,
--     amount/100 as amount,
--     cast(created as timestamp) as created_at

-- from {{ source('stripe', 'payment') }}


with source as (
    select * from {{ source('stripe', 'payment') }}
),
    transformed as (

    select 

        id as payment_id,
        orderid as order_id,
        created as payment_created_at,
        status as payment_status,
        paymentmethod as payment_method,
        round(amount/100.0, 2) as payment_amount
    from source
    
    )

select * from transformed