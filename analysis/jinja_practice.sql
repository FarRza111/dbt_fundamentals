-- Jinja Practice: Common Patterns with Payment Data
-- Learning progressive Jinja techniques

{# 1. Variables - Define reusable values #}
{% set payment_methods = ['credit_card', 'bank_transfer', 'gift_card'] %}
{% set success_statuses = ['success', 'completed'] %}
{% set min_amount = 10 %}

with 
    {# 2. Base data with conditional filtering #}
    payment_base as (
        select 
            * 
        from {{ ref('stg_stripe__payment') }}
        where 
            {# 3. Loop through status options #}
            payment_status in (
                {% for status in success_statuses %}
                '{{ status }}'{% if not loop.last %},{% endif %}
                {% endfor %}
            )
            and payment_amount >= {{ min_amount }}
    ),

    {# 4. Dynamic pivot - most common Jinja pattern #}
    payment_pivoted as (
        select
            order_id,
            
            {# Generate columns for each payment method #}
            {% for method in payment_methods %}
            sum(case when payment_method = '{{ method }}' then payment_amount else 0 end) as {{ method }}_amount,
            count(case when payment_method = '{{ method }}' then 1 end) as {{ method }}_count
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}

        from payment_base
        group by order_id
    ),

    {# 5. Conditional columns based on environment #}
    payment_enhanced as (
        select
            *,
            
            {# Calculate total dynamically #}
            (
            {% for method in payment_methods %}
            {{ method }}_amount{% if not loop.last %} + {% endif %}
            {% endfor %}
            ) as total_amount,
            
            {# Add debug info in development only #}
            {% if target.name == 'dev' %}
            '{{ run_started_at }}' as debug_run_time,
            {% endif %}
            
            {# Determine primary payment method #}
            case 
                {% for method in payment_methods %}
                when {{ method }}_amount > 0 then '{{ method }}'
                {% endfor %}
                else 'other'
            end as primary_method

        from payment_pivoted
    ),

    {# 6. Variable-controlled analysis #}
    payment_analysis as (
        select
            order_id,
            total_amount,
            primary_method,
            
            {# Conditional metrics based on variables #}
            {% if var('include_counts', true) %}
            (
            {% for method in payment_methods %}
            {{ method }}_count{% if not loop.last %} + {% endif %}
            {% endfor %}
            ) as total_transactions,
            {% endif %}
            
            {# Business tier classification #}
            case 
                when total_amount >= 40 then 'premium'
                when total_amount >= 100 then 'standard'
                else 'basic'
            end as customer_tier
            
        from payment_enhanced
        where total_amount > 0
    )

{# 7. Conditional final output #}
{% if var('summary_mode', false) %}
    -- Summary view
    select 
        primary_method,
        count(*) as order_count,
        avg(total_amount) as avg_amount,
        sum(total_amount) as total_revenue
    from payment_analysis
    group by primary_method
{% else %}
    -- Detail view
    select * from payment_analysis
    order by total_amount desc
{% endif %}

{# 
Practice commands to try:
1. dbt compile --vars '{"summary_mode": true}'
2. dbt compile --vars '{"include_counts": false}'  
3. dbt compile --vars '{"summary_mode": true, "include_counts": false}'
#}


