-- Creating task.

CREATE OR REPLACE TASK mytask_minute
  WAREHOUSE = COMPUTE_WH,
  SCHEDULE = '1 MINUTE'
AS
INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);

CREATE TASK mytask_hour
  WAREHOUSE = mywh
  SCHEDULE = 'USING CRON 0 9-17 * * SUN America/Los_Angeles'
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS
INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);

-- SCHEDULE AT SPECIFIC TIME.

# __________ minute (0-59)
# | ________ hour (0-23)
# | | ______ day of month (1-31, or L)
# | | | ____ month (1-12, JAN-DEC)
# | | | | _ day of week (0-6, SUN-SAT, or L)
# | | | | |
# | | | | |
  * * * * *

-- Check sheduled tasks.

SHOW TASKS

-- Put task in the shedule.

alter task mytask_minute resume;

alter task mytask_minute SUSPEND

-- Check task history.

select *
  from table(information_schema.task_history())
  order by scheduled_time;
  
  
-- If any task fails you will not get any failure notification.

-- SCENE 2

CREATE TABLE TASK_1 
(ITERATION NUMBER)

CREATE TABLE TASK_2
(ITERATION NUMBER)

CREATE TABLE TASK_3
(ITERATION NUMBER)

CREATE TABLE TASK_4
(ITERATION NUMBER)

CREATE TABLE TASK_5
(ITERATION NUMBER)

CREATE TABLE TASK_6
(ITERATION NUMBER)

CREATE TABLE TASK_7
(ITERATION NUMBER)

CREATE TASK TASK_1
  WAREHOUSE = COMPUTE_WH,
  SCHEDULE = '1 MINUTE'
AS
INSERT INTO TASK_1 select '1'


CREATE TASK TASK_2
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_1
AS
INSERT INTO TASK_2 select '2'

CREATE TASK TASK_3
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_1
AS
INSERT INTO TASK_3 select '3'


CREATE TASK TASK_4
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_2
AS
INSERT INTO TASK_4 select '4'

CREATE TASK TASK_5
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_2
AS
INSERT INTO TASK_5 select '5'


CREATE TASK TASK_6
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_3
AS
INSERT INTO TASK_6 select '6'

CREATE TASK TASK_7
  WAREHOUSE = COMPUTE_WH
  AFTER TASK_3
AS
INSERT INTO TASK_7 select '7'

SHOW TASKS LIKE 'TASK%'

alter task TASK_1 resume;
alter task TASK_1 suspend;

alter task TASK_2 resume;
alter task TASK_3 resume;

alter task TASK_4 resume;
alter task TASK_5 resume;

alter task TASK_6 resume;
alter task TASK_7 resume;

SELECT * FROM TASK_1

SELECT * FROM TASK_2

SELECT * FROM TASK_3

SELECT * FROM TASK_6

SELECT * FROM TASK_7
