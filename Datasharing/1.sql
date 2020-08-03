create database sales

create table customer as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER

create share sales_s;

grant usage on database sales to share sales_s;

grant usage on schema sales.public to share sales_s;
grant select on table sales.public.customer to share sales_s;

grant all on table sales.public.customer to share sales_s;

show grants to share sales_s;

alter share sales_s add accounts=gia91570;

desc table sales.public.customer
 
create or replace secure view sales.public.customer_data 
as select C_NAME, C_MKTSEGMENT, C_ACCTBAL
from sales.public.customer;

grant usage on database sales to share sales_s;
grant usage on schema sales.public to share sales_s;

grant select on view sales.public.customer_data to
share sales_s;

desc share sales_s;

drop share sales_s

alter share sales_s add accounts=
 
show shares;