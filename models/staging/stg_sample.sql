{{ config(materialized='view') }}

select
    1 as id,
    'test' as name
