---- Table swap

 create or replace table emp_dev (
         first_name string ,
         last_name string ,
         email string ,
         streetaddress string ,
         city string ,
         start_date date
);

copy into emp_dev
from @my_s3_stage
file_format = (type = csv field_optionally_enclosed_by='"')
pattern = '.*employees0[1-5].csv'
ON_ERROR='CONTINUE'

 create or replace table emp_prod (
         first_name string ,
         last_name string ,
         email string ,
         streetaddress string ,
         city string ,
         start_date date
);

copy into emp_prod
from @my_s3_stage
file_format = (type = csv field_optionally_enclosed_by='"')
pattern = '.*employees0[1-2].csv'
ON_ERROR='CONTINUE'

ALTER TABLE  emp_prod SWAP WITH emp_dev

select * from emp_prod

select * from emp_dev