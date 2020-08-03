/****************************************************/

-- Time travel

select * from emp      -- Assume its a production table

update emp set first_name='phc'    -- first wrong update

update emp set city='Bangalore'    -- second wrong update.

select * from emp at(offset => -60*8);    -- after 2 mins you realised you did wrong
                                          -- if you realise after 5 hrs you need to travel back by 5 hrs
                                          -- How long you can travel back depends on table retention period.
                                          

-- Recovery

   create or replace table emp
   as
   select * from emp at(offset => -60*11);
   
-- Big mistake

  -- you realised you need to undo name update also. Try it
  
  select * from emp at(offset => -60*12);   -- not seeing any records
  
  -- Why ?
  
  
-- What you should have been done ?

   -- i will recreate the same table from backup
   
      create or replace table emp
        as
      select * from emp_1
      
update emp set first_name='phc'    -- first wrong update

update emp set city='Bangalore'    -- second wrong update.

-- create backup table
create or replace table emp_bkp
as select * from emp at(offset => -60*7.8);

truncate table emp;

insert into emp
select * from emp_bkp

select * from emp at(offset => -60*16.9);

    -- if you now realise 




create or replace table emp
as select * from emp at(offset => -60*2);

insert into emp
select * from emp_bkp

drop table emp

undrop table emp



-- Time travel

select * from emp

truncate table emp

select * from emp at(offset => -60*5);

create or replace table emp_bkp
as select * from emp at(offset => -60*2);

insert into emp
select * from emp_bkp

create or replace table emp
as
select * from emp_bkp

emp_bkp--- Only 5 min back version.
emp -- you loose all previous version.

drop table emp

undrop table emp


/*********************************************************/