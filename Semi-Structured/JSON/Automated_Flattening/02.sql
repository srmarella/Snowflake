-- Notes:
-- 1. JAVA SP will not return error messages, cannot tell if the something ran fine 
-- 2. There is limit on query size when running automated create view  query 
-- 3. Tested data size 
      -- on prem 5 files 653 MB 
      -- srikanthmarella#LOAD_WH@TEST_JSON_DATA.TEST>PUT file:///C:\Users\srikanth.marella\Documents\GitHub\Snowflake\Semi-Structured\parsedata_json\*.csv @%table_json;
      -- 0_parsedata_json.csv_c.gz(5.93MB): [##########] 100.00% Done (1.524s, 3.89MB/s).
      -- 1_parsedata_json.csv_c.gz(6.12MB): [##########] 100.00% Done (1.276s, 4.80MB/s).
      -- 2_parsedata_json.csv_c.gz(5.92MB): [##########] 100.00% Done (2.176s, 2.72MB/s).
      -- 3_parsedata_json.csv_c.gz(5.85MB): [##########] 100.00% Done (2.219s, 2.64MB/s).
      -- 4_parsedata_json.csv_c.gz(14.59MB): [##########] 100.00% Done (8.574s, 1.70MB/s).
      -- +----------------------+-------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
      -- | source               | target                  | source_size | target_size | source_compression | target_compression | status   | message |
      -- |----------------------+-------------------------+-------------+-------------+--------------------+--------------------+----------+---------|
      -- | 0_parsedata_json.csv | 0_parsedata_json.csv.gz |   123091847 |     6217225 | NONE               | GZIP               | UPLOADED |         |
      -- | 1_parsedata_json.csv | 1_parsedata_json.csv.gz |   123162333 |     6416180 | NONE               | GZIP               | UPLOADED |         |
      -- | 2_parsedata_json.csv | 2_parsedata_json.csv.gz |   121262578 |     6204227 | NONE               | GZIP               | UPLOADED |         |
      -- | 3_parsedata_json.csv | 3_parsedata_json.csv.gz |   124058780 |     6133551 | NONE               | GZIP               | UPLOADED |         |
      -- | 4_parsedata_json.csv | 4_parsedata_json.csv.gz |   193501039 |    15299375 | NONE               | GZIP               | UPLOADED |         |
      -- +----------------------+-------------------------+-------------+-------------+--------------------+--------------------+----------+---------+
      -- 5 Row(s) produced. Time Elapsed: 34.271s

      -- stage size
      -- srikanthmarella#LOAD_WH@TEST_JSON_DATA.TEST>list @%table_json;
      -- +-------------------------+----------+----------------------------------+------------------------------+
      -- | name                    |     size | md5                              | last_modified                |
      -- |-------------------------+----------+----------------------------------+------------------------------|
      -- | 0_parsedata_json.csv.gz |  6217232 | 97f5d404b80bfb0697c2db3b82c68fb5 | Tue, 4 Aug 2020 01:07:19 GMT |
      -- | 1_parsedata_json.csv.gz |  6416192 | 10f3ecfab28a9b56b2457ba981d1c4df | Tue, 4 Aug 2020 01:07:23 GMT |
      -- | 2_parsedata_json.csv.gz |  6204240 | b228870b5d75b9abc05c519653f52e14 | Tue, 4 Aug 2020 01:07:28 GMT |
      -- | 3_parsedata_json.csv.gz |  6133552 | 9d1a55a4cb18ca91d34b5dd7f14e351a | Tue, 4 Aug 2020 01:07:33 GMT |
      -- | 4_parsedata_json.csv.gz | 15299376 | 04eeca40696446b3c719a5f72b62a0a8 | Tue, 4 Aug 2020 01:07:47 GMT |
      -- +-------------------------+----------+----------------------------------+------------------------------+
      -- 5 Row(s) produced. Time Elapsed: 0.230s

      -- table size 
      -- rows 52.2k, size 60.5 MB 

      -- select * from TEST_JSON_DATA.test.table_json_vw; 
         -- scaneed bytes 59.7 MB 
         -- took 6 minutes & 14 s  to run view 
         -- warehouse size x-small 

-- load to table script in dev\01.sql

use role accountadmin;
use database TEST_JSON_DATA;
use schema test;

use warehouse load_wh;



create or replace procedure test_create_view_over_json (TABLE_NAME varchar, COL_NAME varchar, VIEW_NAME varchar)
returns varchar
language javascript
 EXECUTE AS CALLER
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

//define the SP call as a function - it's cleaner this way
//add this function to your stored procs
function log(msg){
    snowflake.createStatement( { sqlText: `call do_log(:1)`, binds:[msg] } ).execute();
}

// var path_name = "regexp_replace(regexp_replace(f.path,'\\\\[(.+)\\\\]'),'(\\\\w+)','\"\\\\1\"')"                           // This generates paths with levels enclosed by double quotes (ex: "path"."to"."element").  It also strips any bracket-enclosed array element references (like "[0]")
var path_name = "f.path"
var attribute_type = "DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING')";    // This generates column datatypes of ARRAY, BOOLEAN, FLOAT, and STRING only
// var alias_name = "REGEXP_REPLACE(REGEXP_REPLACE(f.path, '\\\\[(.+)\\\\]'),'[^a-zA-Z0-9]','_')" ;                           // This generates column aliases based on the path
var alias_name = "f.path" ;                           
var col_list = "";

// Build a query that returns a list of elements which will be used to build the column list for the CREATE VIEW statement
var element_query = "SELECT DISTINCT \n" +
                    path_name + " AS path_name, \n" +
                    // attribute_type + " AS attribute_type, \n" +
                    "1" + " AS attribute_type, \n" +
                    "CONCAT('\"'," +  alias_name + ",'\"')" + " AS alias_name \n" +
                    "FROM \n" + 
                    TABLE_NAME + ", \n" +
                    "LATERAL FLATTEN(" + COL_NAME + ", RECURSIVE=>true) f \n" +
                    "WHERE TYPEOF(f.value) != 'OBJECT' \n";
                    // +
                    // "AND NOT contains(f.path,'[') ";      // This prevents traversal down into arrays;
                    
                    
log(element_query.toString()); //log the value of element_query 

// Run the query...
var element_stmt = snowflake.createStatement({sqlText:element_query});
var element_res = element_stmt.execute();

log(element_res.toString()); //log the value of element_res

// ...And loop through the list that was returned
while (element_res.next()) {

// Add elements and datatypes to the column list
// They will look something like this when added: 
//    col_name:"name"."first"::STRING as name_first, 
//    col_name:"name"."last"::STRING as name_last   

   if (col_list != "") {
      col_list += ", \n";}
   col_list += COL_NAME + ":" + element_res.getColumnValue(1);   // Start with the element path name
   // col_list += "::" + element_res.getColumnValue(2);             // Add the datatype
   col_list += "::" + "String";             // Add the datatype
   col_list += " as " + element_res.getColumnValue(3);           // And finally the element alias 
}

log(col_list.toString()); //log the value of col_list 


// Now build the CREATE VIEW statement
var view_ddl = "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS \n" +
               "SELECT \n" + col_list + "\n" +
               "FROM " + TABLE_NAME;
               
log(view_ddl.toString()); //log the value of view_ddl                

// Now run the CREATE VIEW statement
var view_stmt = snowflake.createStatement({sqlText:view_ddl});
log(view_stmt.toString()); //log the value of view_stmt   

var view_res = view_stmt.execute();
log(view_res.toString()); //log the value of view_res  

return view_res.next();
$$;

