
with 

raw_orders as (

    select  * from {{ ref('stg_orders') }}

),

raw_customers as (

    select * from {{ ref('stg_customers') }}

),


raw_payments as (

    select * from {{ ref('stg_payments') }}

),

completed_payments as (

    select 
        
        ORDERid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
        
    from raw_payments
    where status <> 'fail'
    group by 1

),

paid_orders as (
    
    select 
    
        orders.id as order_id,
        orders.user_id    as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.first_name    as customer_first_name,
        C.last_name as customer_last_name

    from raw_orders as orders
    
    left join completed_payments p ON orders.id = p.order_id

    left join raw_customers C on orders.user_id = C.id 
),

customer_orders as (
    
    select
    
        C.id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    
    from raw_customers C 
    left join raw_orders as orders
    on orders.user_id = C.id 
    group by 1
)

select

    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
    case when c.first_order_date = p.order_placed_at
    then 'new'
    else 'return' end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
    
from paid_orders p
left join customer_orders as c using (customer_id)
LEFT OUTER JOIN (

    select
    
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id

) x on x.order_id = p.order_id

order by order_id