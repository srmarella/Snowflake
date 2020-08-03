use role sysadmin;
use warehouse load_wh;

-- create database and schema 
CREATE OR REPLACE TRANSIENT DATABASE TEST_JSON_DATA DATA_RETENTION_TIME_IN_DAYS = 1;

use database TEST_JSON_DATA;
CREATE OR REPLACE TRANSIENT SCHEMA TEST DATA_RETENTION_TIME_IN_DAYS = 1;

show databases like '%TEST_JSON_DATA%';
show schemas like '%TEST%';

GRANT CREATE SCHEMA, MODIFY, MONITOR, REFERENCE_USAGE, USAGE ON DATABASE "TEST_JSON_DATA" TO ROLE "PUBLIC" WITH GRANT OPTION;
-- GRANT CREATE SCHEMA, MODIFY, MONITOR, REFERENCE_USAGE, USAGE ON SCHEMA "TEST" TO ROLE "PUBLIC" WITH GRANT OPTION;

use role public;
use database TEST_JSON_DATA;
use schema test;


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

CREATE OR REPLACE FILE FORMAT test.Test_CSV_Format
                       Type = 'CSV'
                       FIELD_DELIMITER  = ','
                       FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
                       skip_header = 1;
                      
SHOW FILE FORMATS like '%Test_CSV_Format%'  ;

// PUT file:///C:\Users\srikanth.marella\Documents\GitHub\Snowflake\Semi-Structured\Data\Relational_Json_Data.csv @TEST_JSON_DATA.test.%table_json;

list @%table_json;

select top 1  t.$1, t.$2, t.$3, t.$4, t.$5, t.$6 from '@%table_json' (file_format => test.Test_CSV_Format) t;

COPY INTO test.table_json from '@TEST_JSON_DATA.test.%table_json' file_format =(FORMAT_NAME = test.Test_CSV_Format)  force = true;

select top 100 * from test.table_json;

---------------------------------------------------------------------------------------------------------------------------------------------

-- manually schema on read 

select
    PARSEDATAID,
    PARSEDDATA:BureauName::String AS BureauName,
    PARSEDDATA:ParsedResponse::String AS ParsedResponse,
    PARSEDDATA:ParsedResponse[0]:Key::String AS ParsedResponse_Key,    
    PARSEDDATA:ParsedResponse[0]:TypeName::String AS ParsedResponse_TypeName,    
    PARSEDDATA:ParsedResponse[0]:Value::String AS ParsedResponse_Value,    
    PARSEDDATA 
from test.table_json
WHERE PARSEDATAID = 62123;


-- get all the elements schema 

SELECT DISTINCT
   f.path,
   typeof(f.value)
FROM 
  test.table_json,
  LATERAL FLATTEN(PARSEDDATA, RECURSIVE=>true) f
WHERE
  TYPEOF(f.value) != 'OBJECT';
  
  ----------------------------------------------------------------------------------------------------------------------------------------------
  
  
create or replace procedure create_view_over_json (TABLE_NAME varchar, COL_NAME varchar, VIEW_NAME varchar)
returns varchar
language javascript
as
$$
// CREATE_VIEW_OVER_JSON - Craig Warman, Snowflake Computing, DEC 2019
//
// This stored procedure creates a view on a table that contains JSON data in a column.
// of type VARIANT.  It can be used for easily generating views that enable access to 
// this data for BI tools without the need for manual view creation based on the underlying 
// JSON document structure.  
//
// Parameters:
// TABLE_NAME    - Name of table that contains the semi-structured data.
// COL_NAME      - Name of VARIANT column in the aforementioned table.
// VIEW_NAME     - Name of view to be created by this stored procedure.
//
// Usage Example:
// call create_view_over_json('db.schema.semistruct_data', 'variant_col', 'db.schema.semistruct_data_vw');
//
// Important notes:
//   - This is the "basic" version of a more sophisticated procedure. Its primary purpose
//     is to illustrate the view generation concept.
//   - This version of the procedure does not support:
//         - Column case preservation (all view column names will be case-insensitive).
//         - JSON document attributes that are SQL reserved words (like TYPE or NUMBER).
//         - "Exploding" arrays into separate view columns - instead, arrays are simply
//           materialized as view columns of type ARRAY.
//   - Execution of this procedure may take an extended period of time for very 
//     large datasets, or for datasets with a wide variety of document attributes
//     (since the view will have a large number of columns).
//
// Attribution:
// I leveraged code developed by Alan Eldridge as the basis for this stored procedure.

var path_name = "regexp_replace(regexp_replace(f.path,'\\\\[(.+)\\\\]'),'(\\\\w+)','\"\\\\1\"')"                           // This generates paths with levels enclosed by double quotes (ex: "path"."to"."element").  It also strips any bracket-enclosed array element references (like "[0]")
var attribute_type = "DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING')";    // This generates column datatypes of ARRAY, BOOLEAN, FLOAT, and STRING only
var alias_name = "REGEXP_REPLACE(REGEXP_REPLACE(f.path, '\\\\[(.+)\\\\]'),'[^a-zA-Z0-9]','_')" ;                           // This generates column aliases based on the path
var col_list = "";

// Build a query that returns a list of elements which will be used to build the column list for the CREATE VIEW statement
var element_query = "SELECT DISTINCT \n" +
                    path_name + " AS path_name, \n" +
                    attribute_type + " AS attribute_type, \n" +
                    alias_name + " AS alias_name \n" +
                    "FROM \n" + 
                    TABLE_NAME + ", \n" +
                    "LATERAL FLATTEN(" + COL_NAME + ", RECURSIVE=>true) f \n" +
                    "WHERE TYPEOF(f.value) != 'OBJECT' \n" +
                    "AND NOT contains(f.path,'[') ";      // This prevents traversal down into arrays;

// Run the query...
var element_stmt = snowflake.createStatement({sqlText:element_query});
var element_res = element_stmt.execute();

// ...And loop through the list that was returned
while (element_res.next()) {

// Add elements and datatypes to the column list
// They will look something like this when added: 
//    col_name:"name"."first"::STRING as name_first, 
//    col_name:"name"."last"::STRING as name_last   

   if (col_list != "") {
      col_list += ", \n";}
   col_list += COL_NAME + ":" + element_res.getColumnValue(1);   // Start with the element path name
   col_list += "::" + element_res.getColumnValue(2);             // Add the datatype
   col_list += " as " + element_res.getColumnValue(3);           // And finally the element alias 
}

// Now build the CREATE VIEW statement
var view_ddl = "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS \n" +
               "SELECT \n" + col_list + "\n" +
               "FROM " + TABLE_NAME;

// Now run the CREATE VIEW statement
var view_stmt = snowflake.createStatement({sqlText:view_ddl});
var view_res = view_stmt.execute();
return view_res.next();
$$;




----------------------------------------------------------------------------------------------------------------------------------------------


call create_view_over_json('TEST_JSON_DATA.test.table_json', 'PARSEDDATA', 'TEST_JSON_DATA.test.table_json_vw');

-- Created view 
CREATE OR REPLACE VIEW TEST_JSON_DATA.test.table_json_vw AS 
SELECT 
PARSEDDATA:"BureauName"::STRING as BureauName, 
PARSEDDATA:"ParsedResponse"::ARRAY as ParsedResponse, 
PARSEDDATA:"PrimaryKey"::FLOAT as PrimaryKey
FROM TEST_JSON_DATA.test.table_json;


select * from TEST_JSON_DATA.test.table_json_vw;