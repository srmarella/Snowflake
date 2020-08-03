
create table test_duplicates_test (PATH_NAME string,ATTRIBUTE_TYPE string,ALIAS_NAME string )
as
SELECT DISTINCT 
f.path AS path_name, 
DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING') AS attribute_type, 
CONCAT('"',f.path,'"') AS alias_name 
FROM 
TEST_JSON_DATA.test.table_json, 
LATERAL FLATTEN(PARSEDDATA, RECURSIVE=>true) f 
WHERE TYPEOF(f.value) != 'OBJECT' ;



select * from TEST_JSON_DATA.test.table_json_vw;

select
    PATH_NAME, ALIAS_NAME , attribute_type, count(*) 
from test_duplicates_test
group by PATH_NAME, ALIAS_NAME,  attribute_type 
HAVING COUNT(*) >1;