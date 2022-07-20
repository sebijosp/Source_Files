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

SET WHERE_clause;

INSERT INTO TABLE cdrdm.FACT_IMS_DROP_CDR PARTITION (Start_Date)
SELECT
Drop_Code,
cast(trim(Served_party_number) as bigint),
CONCAT(SUBSTR(Start_date_time,1,4),'-',SUBSTR(Start_date_time,5,2),'-',SUBSTR(Start_date_time,7,2),' ',SUBSTR(Start_date_time,9,2),':',SUBSTR(Start_date_time,11,2),':',SUBSTR(Start_date_time,13,2)) as Start_date_time,
CONCAT(SUBSTR(Answered_date_time,1,4),'-',SUBSTR(Answered_date_time,5,2),'-',SUBSTR(Answered_date_time,7,2),' ',SUBSTR(Answered_date_time,9,2),':',SUBSTR(Answered_date_time,11,2),':',SUBSTR(Answered_date_time,13,2)) as Answered_date_time,
CONCAT(SUBSTR(Release_date_time,1,4),'-',SUBSTR(Release_date_time,5,2),'-',SUBSTR(Release_date_time,7,2),' ',SUBSTR(Release_date_time,9,2),':',SUBSTR(Release_date_time,11,2),':',SUBSTR(Release_date_time,13,2)) as Release_date_time,
Switch_id,
cast(Call_action_code as int),
cast(Call_terminate_code as int),
cast(trim(Other_party_number) as bigint),
Service_type,
Utc_offset,
Tariff_class,
Access_type,
Presentation_ind,
Unique_session_id,
Served_party_device_id,
Unique_record_id,
Partial_call_ind,
Tele_service_code,
Cell_identifier,
Call_type,
Call_pull_flag,
Dialled_digits,
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
from_unixtime(unix_timestamp()),
CONCAT(SUBSTR(Start_date_time,1,4),'-',SUBSTR(Start_date_time,5,2),'-',SUBSTR(Start_date_time,7,2)) as Start_Date
FROM cdrdm.ims1205drop a ${hiveconf:WHERE_clause};