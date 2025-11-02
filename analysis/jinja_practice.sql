WITH 
    payment 
AS (
    select 
    * 
    from {{ ref('stg_stripe__payment') }}
    where 
        payment_status = 'success'
)
, pivoted AS 
(
    select 
        order_id
        , sum(case when payment_method = 'bank_transfer' then payment_amount else 0 end) as bank_transfer_amount
    from payment
    group by order_id
)


select * from pivoted
