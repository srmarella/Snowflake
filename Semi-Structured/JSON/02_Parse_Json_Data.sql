-- set context 
use role
public;
use database TEST_JSON_DATA;
use schema test;


select top 2
    * , parse_json(PARSEDDATA)
from test.table_json;

-- simple select 
select
    PARSEDATAID, parse_json(PARSEDDATA)
from test.table_json
WHERE PARSEDATAID = '62122';

-- explode
select
    -- t.PARSEDATAID
    -- ,parse_json(t.PARSEDDATA) 
    distinct lf.PATH
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf
WHERE PARSEDATAID = '62122';

-- explode
select
    t.PARSEDATAID
   -- ,parse_json(t.PARSEDDATA) 
   , lf.*
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf
WHERE PARSEDATAID = '62122';


-- explode and remove the KEY with null just to get the parsing data 
select
    t.PARSEDATAID
   -- ,parse_json(t.PARSEDDATA) 
   , lf.*
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf 
WHERE t.PARSEDATAID = '62122'
    AND lf.KEY IS NOT NULL;

select
    t.PARSEDATAID
   -- ,parse_json(t.PARSEDDATA) 
   
   -- recursive fallten 
   , lf.*
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf 
WHERE t.PARSEDATAID = '62122'
    AND lf.KEY IS NOT NULL;

select
    t.PARSEDATAID
   -- ,parse_json(t.PARSEDDATA) 
   
   -- manual parsing
   , t.
PARSEDDATA:
BureauName
   ,t.
PARSEDDATA:
ParsedResponse[0].Key
   ,t.
PARSEDDATA:
ParsedResponse[20].Value.PrimaryCustomer.Address.City
   ,t.
Parseddata:
ParsedResponse[1710].Value
from test.table_json as t
    -- ,lateral flatten(input=> t.PARSEDDATA, RECURSIVE => TRUE) as lf 
WHERE t.PARSEDATAID = '62122';
-- AND lf.KEY IS NOT NULL;



select
    lf.KEY
   , lf.PATH
   , lf.VALUE
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf 
WHERE t.PARSEDATAID = '62122'
    AND lf.KEY IS NOT NULL;

select
    lf.seq
   , lf.KEY
   , lf.PATH
   -- ,regexp_replace(lf.PATH, '\\[[0-9]+\\]','') as clean_path
   , regexp_count(lf.path,'\\.|\\[') +1 as level
   , typeof(lf.value) as "Type"
   --,lf.index
   , lf.value as "Current Level Value"
--,lf.this as "Above Level Value" 
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf 
WHERE t.PARSEDATAID = '62122'
    AND "Type" not in
('OBJECT','ARRAY')
    AND Key not in
('TypeName');


create or replace transient table test.data_two
as
select
    lf.seq
   , lf.KEY
   , lf.PATH
   -- ,regexp_replace(lf.PATH, '\\[[0-9]+\\]','') as clean_path
   , regexp_substr(lf.PATH, '[0-9]+') as PATH_SEQ
   , regexp_count(lf.path,'\\.|\\[') +1 as level
   , typeof(lf.value) as "Type"
   --,lf.index
   , lf.value as "Current Level Value"
   --,lf.this as "Above Level Value" 
   , case when lf.key = 'Key' then lf.value else '' end as KEY_New
   , case when lf.key = 'Key' then 0 else 1 end as Key_Or_Value 
   , NULL AS Value_New
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf 
WHERE t.PARSEDATAID = '62122'
    AND "Type" not in
('OBJECT','ARRAY')
    AND Key not in
('TypeName')    
    AND KEY not in
('Key', 'Value');


create or replace transient table test.data 
as
select
    lf.seq
   , lf.KEY
   , lf.PATH
   -- ,regexp_replace(lf.PATH, '\\[[0-9]+\\]','') as clean_path
   , regexp_substr(lf.PATH, '[0-9]+') as PATH_SEQ
   , regexp_count(lf.path,'\\.|\\[') +1 as level
   , typeof(lf.value) as "Type"
   --,lf.index
   , lf.value as "Current Level Value"
   --,lf.this as "Above Level Value" 
   , case when lf.key = 'Key' then lf.value else '' end as KEY_New
   , case when lf.key = 'Key' then 0 else 1 end as Key_Or_Value 
   , NULL AS Value_New
from test.table_json as t
    , lateral flatten(input
=> t.PARSEDDATA, RECURSIVE => TRUE) as lf 
WHERE t.PARSEDATAID = '62122'
    AND "Type" not in
('OBJECT','ARRAY')
    AND Key not in
('TypeName')    
    AND KEY in
('Key', 'Value');


select t.*, s.*
from test.data as t
    left join lateral(
        select "CURRENT LEVEL VALUE"  as VALUE_NEW from test.data as i
where i.PATH_SEQ = t.PATH_SEQ
    and i.KEY_OR_VALUE = 1
) as s
WHERE t.KEY_OR_VALUE = 0;


update test.data
set VALUE_NEW = s.VALUE_NEW
from test.data t
    left join lateral(
        select "CURRENT LEVEL VALUE"  as VALUE_NEW , PATH_SEQ
from test.data as i
        where i.PATH_SEQ = t.PATH_SEQ
        and i.KEY_OR_VALUE = 1
    ) as s
WHERE t.KEY_OR_VALUE = 0
and s.PATH_SEQ = t.PATH_SEQ;

update test.data
set VALUE_NEW = s.VALUE_NEW
from (
        select "CURRENT LEVEL VALUE" as VALUE_NEW , PATH_SEQ
    from test.data as i
    WHERE  i.KEY_OR_VALUE = 1
    ) as s
WHERE KEY_OR_VALUE = 0 and s.PATH_SEQ = test.data.PATH_SEQ;



    select KEY_NEW, VALUE_NEW
    from test.data
    where KEY_OR_VALUE = 0
union
    select KEY, "CURRENT LEVEL VALUE"
    from test.data_two;
    
    
    