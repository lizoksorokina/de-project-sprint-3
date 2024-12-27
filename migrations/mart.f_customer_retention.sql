insert into mart.f_customer_retention (
    new_customers_count, 
    returning_customers_count, 
    refunded_customer_count, 
    period_name, 
    period_id, 
    item_id, 
    new_customers_revenue, 
    returning_customers_revenue, 
    customers_refunded
)
select 
    count(distinct case when status = 'new' then customer_id end) as new_customers_count,
    count(distinct case when status <> 'new' then customer_id end) as returning_customers_count,
    count(distinct case when status = 'refunded' then customer_id end) as refunded_customer_count,
    'weekly' as period_name,
    weekly as period_id,
    item_id,
    sum(case when status = 'new' then payment_amount end) as new_customers_revenue,
    sum(case when status <> 'new' then payment_amount end) as returning_customers_revenue,
    sum(case when status = 'refunded' then quantity end) as customers_refunded
from (
    select 
        date_part('week', c.date_actual) as weekly,
        u.customer_id,
        s.quantity,
        s.payment_amount,
        case 
            when count(u.customer_id) over (partition by u.customer_id, date_part('week', c.date_actual)) = 1 
            then 'new' 
            else s.status 
        end as status,
        s.item_id
    from mart.f_sales s
    join mart.d_customer u on s.customer_id = u.customer_id
    join mart.d_calendar c on s.date_id = c.date_id
) as prep
group by weekly, item_id
having weekly = date_part('week', '{{ds}}'::date);
