{{
  config(
    materialized = 'incremental',
    unique_key = 'contracting_officers_determination_of_business_size_code',
    incremental_strategy='delete+insert'
  )
}}

with business_size as (
    
    select 
        DISTINCT contracting_officers_determination_of_business_size_code ,
                 contracting_officers_determination_of_business_size 


    from {{ source("spend_data", "award_details") }}
    where contracting_officers_determination_of_business_size_code is not null and contracting_officers_determination_of_business_size is not null
)

select * from business_size