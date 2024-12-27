drop table if exists mart.f_customer_retention
create table if not exists mart.f_customer_retention (
    id serial PRIMARY KEY,
    new_customers_count bigint,
    returning_customers_count bigint,
    refunded_customer_count bigint,
    period_name varchar(50),
    period_id varchar(50),
    item_id varchar(50),
    new_customers_revenue float,
    returning_customers_revenue float,
    customers_refunded bigint
);
