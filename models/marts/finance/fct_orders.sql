
{{
  config(
    enabled=false,
    materialized='external'
  )
}}

with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payment') }} 
),


order_payments as (
    select 
        order_id,
        sum(case when status = 'success' then amount end) as amount 
    from payments 
    group by order_id
),


final as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date
    from orders
    left join order_payments using (order_id)


)

select * from final