CREATE OR REPLACE FILE FORMAT CSV_TYPE
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  error_on_column_count_mismatch=false;

create stage csv_new_type
file_format=CSV_TYPE;

  list @CSV_STAGE;
--type 0
  CREATE OR REPLACE TABLE STG_Employee
(
  EID VARCHAR,
  EName VARCHAR,
  Email VARCHAR,
  Phoneno VARCHAR,
  Address VARCHAR,
  Companyname VARCHAR,
  Exp INT
);

--type 1
  CREATE OR REPLACE TABLE Employee_type1
(
  EID VARCHAR,
  EName VARCHAR,
  Email VARCHAR,
  Phoneno VARCHAR,
  Address VARCHAR,
  Companyname VARCHAR,
  Exp INT
);
--type 2

CREATE OR REPLACE TABLE DIM_EMPLOYEE
(
  EID VARCHAR,
  EName VARCHAR,
  Email VARCHAR,
  Phoneno VARCHAR,
  Address VARCHAR,
  Companyname VARCHAR,
  Exp INT,
  Start_date DATE,
  End_date DATE DEFAULT DATE '9999-12-31',
  IS_ACTIVE BOOLEAN
);
--parent task
CREATE OR REPLACE TASK CLEAN_STAGE_TABLE
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 minute'
AS
  TRUNCATE TABLE STG_Employee;
--child task
CREATE OR REPLACE TASK LOAD_STAGE_DATA
  WAREHOUSE = COMPUTE_WH
  AFTER CLEAN_STAGE_TABLE
AS
  COPY INTO STG_Employee
  FROM @csv_new_type;

  select $1,$2,$3,$4,$5,$6,$7 from @CSV_STAGE;
  
  ---child task
  CREATE OR REPLACE TASK EMP_SCD1_LOAD
  WAREHOUSE = COMPUTE_WH
  AFTER LOAD_STAGE_DATA
AS
  MERGE INTO Employee_type1 emp
  USING (SELECT * FROM STG_Employee) src
  ON emp.ENAME = src.ENAME 
     AND emp.EID = src.EID
  WHEN MATCHED 
    AND (emp.EMAIL != src.EMAIL 
         OR emp.PHONENO != src.PHONENO 
         OR emp.ADDRESS != src.ADDRESS)
  THEN UPDATE SET 
       emp.EMAIL = src.EMAIL,
       emp.PHONENO = src.PHONENO,
       emp.ADDRESS = src.ADDRESS
  WHEN NOT MATCHED
  THEN INSERT (EID, ENAME, EMAIL, PHONENO, ADDRESS, COMPANYNAME, EXP)
       VALUES (src.EID, src.ENAME, src.EMAIL, src.PHONENO, src.ADDRESS, src.COMPANYNAME, src.EXP);

select * from STG_Employee;

select * from Employee_type1;


select * from DIM_EMPLOYEE;

show tasks;

ALTER TASK CLEAN_STAGE_TABLE SUSPEND;
ALTER TASK LOAD_STAGE_DATA SUSPEND;
ALTER TASK EMP_SCD1_LOAD SUSPEND;
ALTER TASK DIM_EMP_LOAD SUSPEND;

ALTER TASK CLEAN_STAGE_TABLE resume;
ALTER TASK LOAD_STAGE_DATA resume;
ALTER TASK EMP_SCD1_LOAD resume;
ALTER TASK DIM_EMP_LOAD resume;

select * from table(information_schema.task_history());

CREATE OR REPLACE STREAM STR_EMP 
ON TABLE Employee_type1;

select * from STR_EMP;
---final task to load the data into scd type2
CREATE OR REPLACE TASK DIM_EMP_LOAD
  WAREHOUSE = COMPUTE_WH
  AFTER EMP_SCD1_LOAD
  WHEN SYSTEM$STREAM_HAS_DATA('STR_EMP')
AS
  MERGE INTO DIM_EMPLOYEE emp
  USING (SELECT * FROM STR_EMP) chk
  ON emp.EID = chk.EID
     AND emp.Address = chk.Address
  WHEN MATCHED 
    AND (chk.METADATA$ACTION = 'DELETE')
  THEN UPDATE SET End_date = CURRENT_DATE,
                  IS_ACTIVE = FALSE
WHEN NOT MATCHED 
  AND (chk.METADATA$ACTION = 'INSERT')
THEN INSERT (EID, EName, Email, Phoneno, Address, Companyname, Exp, Start_date, IS_ACTIVE)
     VALUES (chk.EID, chk.EName, chk.Email, chk.Phoneno, chk.Address, chk.Companyname, chk.Exp, CURRENT_DATE, TRUE);

CREATE OR REPLACE TASK DIM_EMP_LOAD
  WAREHOUSE = COMPUTE_WH
  AFTER EMP_SCD1_LOAD
  WHEN SYSTEM$STREAM_HAS_DATA('STR_EMP')
AS
  MERGE INTO DIM_EMPLOYEE emp
  USING (SELECT * FROM STR_EMP) chk
  ON emp.EID = chk.EID
     AND emp.Address = chk.Address

  -- Close out old record when a delete occurs (part of update or true delete)
  WHEN MATCHED 
    AND chk.METADATA$ACTION = 'DELETE'
  THEN UPDATE SET End_date = CURRENT_DATE,
                  IS_ACTIVE = FALSE

  -- Insert new record when an insert occurs (new row or updated row)
  WHEN NOT MATCHED 
    AND chk.METADATA$ACTION = 'INSERT'
  THEN INSERT (EID, EName, Email, Phoneno, Address, Companyname, Exp, Start_date, IS_ACTIVE)
       VALUES (chk.EID, chk.EName, chk.Email, chk.Phoneno, chk.Address, chk.Companyname, chk.Exp, CURRENT_DATE, TRUE);













