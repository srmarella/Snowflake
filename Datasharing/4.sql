## Run below commands in parent account

-- Expose database to reader account using share.

create database reader_sales

create table customer as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER

create share sales_s;

grant usage on database reader_sales to share sales_s;
grant usage on schema reader_sales.public to share sales_s;
grant select on table reader_sales.public.customer to share sales_s;

grant usage on database reader_sales to role public;
grant usage on schema reader_sales.public to role public;
grant select on table reader_sales.public.customer to role public;

grant usage on warehouse reader_wh to role public;

grant all on  warehouse reader_wh to role public;

alter share sales_s add accounts=qxa57318;

## Run below commands in reader account.

-- Create reference database 

drop database reader_sales
create database reader_sales from share gia91570.sales_s;

-- Create warehouse for reader account

create or replace warehouse reader_wh with
warehouse_size='X-SMALL'
auto_suspend = 180
auto_resume = true
initially_suspended=true;

grant usage on warehouse reader_wh to role public;
grant usage on warehouse reader_wh to role sysadmin;
grant all on warehouse reader_wh to  sysadmin;

select * from customer