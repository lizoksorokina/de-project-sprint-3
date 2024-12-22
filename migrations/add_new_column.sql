alter table staging.user_order_log
add column if not exists status varchar(50) not null
default 'shipped';

alter table mart.f_sales
add column if not exists status varchar(50) not null default 'shipped';