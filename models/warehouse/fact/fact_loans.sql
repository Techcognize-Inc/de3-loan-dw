{{ config(materialized='table') }}

select
    1 as loan_id,
    1 as customer_id,
    1000 as loan_amount,
    'Y' as target
