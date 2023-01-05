with

    final as (

    select
    
        *
    
    from {{ source('stripe', 'payments') }}

)

select * from final