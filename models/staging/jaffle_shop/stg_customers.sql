with

    final as (

    select
    
        *
    
    from {{ source('jaffle_shop', 'customers') }}

)

select * from final