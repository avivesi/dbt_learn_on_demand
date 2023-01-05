with

    final as (

    select
    
        *
    
    from {{ source('jaffle_shop', 'orders') }}

)

select * from final