/****** Try creating database in reader account *******/

create or replace database sales

create or replace table customer_test like reader_sales.public.customer

insert into customer_test
select * from
reader_sales.public.customer;

/********Try doing clone *******/

create or replace table customer_test clone reader_sales.public.customer;


/****** try creating dummy table and insert value **********/

create table people (name varchar)

insert into people
select 'prakash'