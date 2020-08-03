create or replace secure view sales.public.customer_data 
as select C_NAME, C_MKTSEGMENT, C_ACCTBAL
from sales.public.customer;