CREATE OR REPLACE TRANSIENT DATABASE STAGING_AREA

 create or replace table emp (
         first_name string ,
         last_name string ,
         email string ,
         streetaddress string ,
         city string ,
         start_date date
);

SHOW TABLES

show tables history;

ALTER DATABASE STAGING_AREA SET DATA_RETENTION_TIME_IN_DAYS = 0;