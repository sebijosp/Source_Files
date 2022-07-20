SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;

CREATE TABLE cdrdm.Z_FACT_IMS_CDR_Temp(
Served_Party_Number BIGINT,
Other_Party_Number BIGINT,
Start_Date_Time STRING,
Answered_Date_Time STRING,
End_Date_Time STRING,
Duration STRING,
Switch_ID CHAR(7),
Call_Code STRING,
Termination_Code INT,
Service_Type VARCHAR(200),
Time_Zone VARCHAR(10),
Tariff_Class CHAR(4),
Access_Type CHAR(3),
Presentation_Indicator CHAR(4),
Device_ID VARCHAR(81),
Session_ID VARCHAR(60),
Record_ID VARCHAR(60),
Partial_Call_Indicator CHAR(4),
Cell_ID VARCHAR(41),
Call_Type CHAR(4),
Call_Pull_Flag CHAR(4),
Dialed_Digits VARCHAR(20),
TeleService_Code CHAR(4),
--Audit_key INT,
--Time_key STRING,
file_name STRING,
record_start BIGINT,
record_end BIGINT,
record_type STRING,
family_name STRING,
version_id INT,
file_time STRING,
file_id STRING,
switch_name STRING,
num_records STRING,
insert_timestamp STRING)
PARTITIONED BY (End_Date STRING) --LES Report refers to End_Date vs Start_Date (TD Partition)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY");

INSERT INTO TABLE cdrdm.Z_FACT_IMS_CDR_Temp PARTITION (End_Date) 
SELECT 
served_party_number,
other_party_number,
start_date_time,
answered_date_time,
end_date_time,
duration,
switch_id,
call_code,
termination_code,
service_type,
time_zone,
tariff_class,
access_type,
presentation_indicator,
device_id,
session_id,
record_id,
partial_call_indicator,
cell_id,
call_type,
call_pull_flag,
dialed_digits,
teleservice_code,
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records,
insert_timestamp,
end_date
FROM cdrdm.FACT_IMS_CDR where file_name <> 'FACT_IMS_CDR';

INSERT INTO TABLE cdrdm.Z_FACT_IMS_CDR_Temp PARTITION (End_Date) 
SELECT
served_party_number,
other_party_number,
start_date_time,
answered_date_time,
end_date_time,
CONCAT(substr(lpad(cast(cast((unix_timestamp(End_Date_Time) - unix_timestamp(Answered_Date_Time)) as int) as string), 5, '0'),1,1),':', substr(lpad(cast(cast((unix_timestamp(End_Date_Time) - unix_timestamp(Answered_Date_Time)) as int) as string), 5, '0'),2,2),':', substr(lpad(cast(cast((unix_timestamp(End_Date_Time) - unix_timestamp(Answered_Date_Time)) as int) as string), 5, '0'),4,2)) as duration,
switch_id,
call_code,
termination_code,
service_type,
time_zone,
tariff_class,
access_type,
presentation_indicator,
device_id,
session_id,
record_id,
partial_call_indicator,
cell_id,
call_type,
call_pull_flag,
dialed_digits,
teleservice_code,
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records,
insert_timestamp,
end_date
FROM cdrdm.FACT_IMS_CDR where file_name = 'FACT_IMS_CDR';