/***************Secure view normal view difference **************/

create or replace secure view sales.public.customer_data_secure 
as select C_NAME, C_MKTSEGMENT, C_ACCTBAL
from sales.public.customer;


create or replace view sales.public.customer_data_normal
as select C_NAME, C_MKTSEGMENT, C_ACCTBAL
from sales.public.customer;

show views like  '%cust%'

grant usage on database sales to role public
grant usage on schema sales.public to role public

grant select on sales.public.customer_data_normal to role public;
grant select on sales.public.customer_data_secure to role public;


/********* How data might get exposed in normal view ***********/

create or replace view sales.public.customer_data_normal
as select C_NAME, C_MKTSEGMENT, C_ACCTBAL
from sales.public.customer where c_mktsegment='AUTOMOBILE'

create or replace secure view sales.public.customer_data_secure
as select C_NAME, C_MKTSEGMENT, C_ACCTBAL
from sales.public.customer where c_mktsegment='AUTOMOBILE'

select * from sales.public.customer where c_mktsegment='AUTOMOBILE' limit 100

select * from sales.public.customer limit 100


grant select on sales.public.customer_data_normal to role public;
grant select on sales.public.customer_data_secure to role public;
grant usage on warehouse know_architecture_1 to role public