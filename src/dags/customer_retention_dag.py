from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'customer_retention_dag',
    default_args=default_args,
    description='DAG для обновления таблицы f_customer_retention',
    schedule_interval='@weekly',  
    catchup=False,
)
delete_task = PostgresOperator(
    task_id='delete_customer_retention',
    sql="""
       delete from mart.f_customer_retention where period_id = (DATE_PART('week', '{{ds}}'::date))::text;
       delete from mart.f_customer_retention where period_id = (DATE_PART('week', '{{ds}}'::date) - 1)::text;

    """,
    postgres_conn_id='postgresql_de',
    autocommit=True,
    dag=dag,
)

# Задача для вставки данных в таблицу f_customer_retention
insert_task = PostgresOperator(
    task_id='insert_customer_retention',
    sql="""
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

    """,
    postgres_conn_id='postgresql_de',
    autocommit=True,
    dag=dag,
)
delete_task >> insert_task
