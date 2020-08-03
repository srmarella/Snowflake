use role sysadmin;

-- create database and schema 
CREATE OR REPLACE TRANSIENT DATABASE TEST_JSON_DATA DATA_RETENTION_TIME_IN_DAYS = 1;
CREATE OR REPLACE TRANSIENT SCHEMA TEST DATA_RETENTION_TIME_IN_DAYS = 1;

show databases like '%TEST_JSON_DATA%'

-- permissions
GRANT CREATE SCHEMA, MODIFY, MONITOR, REFERENCE_USAGE, USAGE ON DATABASE "TEST_JSON_DATA" TO ROLE "PUBLIC" WITH GRANT OPTION;

-- set context 
use role public;
use database TEST_JSON_DATA;
use schema test;

-- test data creation 
CREATE OR REPLACE TRANSIENT TABLE test.table_json 
                      (ParseDataId VARCHAR
                       ,DecisionResultId VARCHAR
                       ,AccountId VARCHAR
                       ,BureauId VARCHAR
                       ,PrimaryKey VARCHAR
                       ,ParsedData VARIANT
                       ,CreatedDate VARCHAR(100)
                       ,CreatedByName VARCHAR(100))
                       DATA_RETENTION_TIME_IN_DAYS = 1;
                       
show tables like '%table_json%';
select * from test.table_json;

-- file format 
CREATE OR REPLACE FILE FORMAT test.Test_CSV_Format
                       Type = 'CSV'
                       FIELD_DELIMITER  = ','
                       FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
                       skip_header = 1;
                      
SHOW FILE FORMATS like '%Test_CSV_Format%'                      ;

// from snowsql
// snowsql -a progleasing_qas.west-us-2.azure -u srikanthmarella
// 
// Sri#LOAD_WH@TEST_JSON_DATA.TEST> PUT file:///C:\Users\srikanth.marella\Documents\GitHub\Snowflake\Semi-Structured\Data\Relational_Json_Data.csv @TEST_JSON_DATA.test.%table_json;
// Relational_Json_Data.csv_c.gz(0.86MB): [##########] 100.00% Done (0.919s, 0.93MB/s).
// +--------------------------+-----------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
// | source                   | target                      | source_size | target_size | source_compression | target_compression | status   | message |
// |--------------------------+-----------------------------+-------------+-------------+--------------------+--------------------+----------+---------|
// | Relational_Json_Data.csv | Relational_Json_Data.csv.gz |    29428343 |      900434 | NONE               | GZIP               | UPLOADED |         |
// +--------------------------+-----------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
// 1 Row(s) produced. Time Elapsed: 3.276s

// 
// 
// Sri#LOAD_WH@TEST_JSON_DATA.TEST>list @%table_json;
// +-----------------------------+--------+----------------------------------+-------------------------------+
// | name                        |   size | md5                              | last_modified                 |
// |-----------------------------+--------+----------------------------------+-------------------------------|
// | Relational_Json_Data.csv.gz | 900448 | 4be5cb6989e9756ec94a19cb7c3d5c2e | Tue, 21 Jul 2020 20:49:51 GMT |
// +-----------------------------+--------+----------------------------------+-------------------------------+
// 1 Row(s) produced. Time Elapsed: 0.867s
// 
// 
// Sri#LOAD_WH@TEST_JSON_DATA.TEST>select top 1  t.$1, t.$2, t.$3, t.$4, t.$5, t.$6 from '@%table_json' (file_format => test.Test_CSV_Format) t;
// 
// 

COPY INTO test.table_json from '@TEST_JSON_DATA.test.%table_json' file_format =(FORMAT_NAME = test.Test_CSV_Format)  force = true;

select top 100 * from test.table_json;



