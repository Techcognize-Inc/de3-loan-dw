{{ config(materialized='table') }}

select
    1 as customer_id,
    'test_customer' as customer_name
