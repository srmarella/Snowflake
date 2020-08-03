use role accountadmin;
use database TEST_JSON_DATA;
use schema test;

use warehouse load_wh;


set do_log = true; --true to enable logging, false (or undefined) to disable
set log_table = 'my_log_table'; --The name of the temp table where log messages go


CREATE or replace PROCEDURE do_log(MSG STRING)
 RETURNS STRING
 LANGUAGE JAVASCRIPT
 EXECUTE AS CALLER
AS $$
 
 //see if we should log - checks for do_log = true session variable
 try{
    var foo = snowflake.createStatement( { sqlText: `select $do_log` } ).execute();
 } catch (ERROR){
    return; //swallow the error, variable not set so don't log
 }
 foo.next();
 if (foo.getColumnValue(1)==true){ //if the value is anything other than true, don't log
    try{
        snowflake.createStatement( { sqlText: `create temp table identifier ($log_table) if not exists (ts number, msg string)`} ).execute();
        snowflake.createStatement( { sqlText: `insert into identifier ($log_table) values (:1, :2)`, binds:[Date.now(), MSG] } ).execute();
    } catch (ERROR){
        throw ERROR;
    }
 }
 $$
;


CREATE or replace PROCEDURE my_test()
 RETURNS STRING
 LANGUAGE JAVASCRIPT
 EXECUTE AS CALLER
AS $$

//define the SP call as a function - it's cleaner this way
//add this function to your stored procs
function log(msg){
    snowflake.createStatement( { sqlText: `call do_log(:1)`, binds:[msg] } ).execute();
}

//now just call the log function anytime...
try{
    var x = 10/10;
    log('log this message'); //call the log function
    //do some stuff here
    log('x = ' + x.toString()); //log the value of x 
    log('this is another log message'); //throw in another log message
}catch(ERROR){
    log(ERROR); //we can even catch/log the error messages
    return ERROR;
}

 $$
;


call my_test();


select * from my_log_table order by 1 desc;

call test_create_view_over_json('TEST_JSON_DATA.test.table_json', 'PARSEDDATA', 'TEST_JSON_DATA.test.table_json_vw');
select *
from TEST_JSON_DATA.test.table_json_vw;


SELECT DISTINCT 
f.path AS path_name, 
DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING') AS attribute_type, 
f.path AS alias_name 
FROM 
TEST_JSON_DATA.test.table_json, 
LATERAL FLATTEN(PARSEDDATA, RECURSIVE=>true) f 
WHERE TYPEOF(f.value) != 'OBJECT' ;



