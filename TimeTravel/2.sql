--- Set retention time

create or replace table emp_retention (
         first_name varchar(10) ,
         last_name varchar(10) ,
         email varchar(10) ,
         streetaddress varchar(100) ,
         city varchar(100) ,
         start_date string
);

copy into emp_retention
from @my_s3_stage
file_format = (type = csv field_optionally_enclosed_by='"')
pattern = '.*employees0[1-5].csv'
ON_ERROR='CONTINUE'

-- Check retention period.

SHOW TABLES LIKE 'emp_retention' in sfnowflake_tutorial.public

SHOW TABLES LIKE 'emp' in sfnowflake_tutorial.public

-- Retention period is set.

drop table emp

undrop table emp

select * from emp

-- Retention period is unset.

ALTER TABLE emp_retention SET DATA_RETENTION_TIME_IN_DAYS = 0;

drop table emp_retention

undrop table emp_retention

select * from emp_retention at(offset => -60*5);

--unset retention period for emp

SHOW TABLES LIKE 'emp' in sfnowflake_tutorial.public

ALTER TABLE emp SET DATA_RETENTION_TIME_IN_DAYS = 0;

SHOW TABLES LIKE 'emp' in sfnowflake_tutorial.public

drop table emp

undrop table emp

--set retention period for emp

ALTER TABLE emp SET DATA_RETENTION_TIME_IN_DAYS = 1;

SHOW TABLES LIKE 'emp' in sfnowflake_tutorial.public

drop table emp
undrop table emp
