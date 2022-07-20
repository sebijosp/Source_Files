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

INSERT INTO TABLE cdrdm.FACT_GLENAYRE_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
cast(if(upper(trim(Subscriber_No))='ZZZZZZZZZZ',NULL,trim(Subscriber_No)) as bigint) as subscriber_no,
Subscriber_No as subscriber_no_char,
if(substr(Channel_Seizure_Date,9,2) != 'ZZ', concat(substr(Channel_Seizure_Date,9,2),':',substr(Channel_Seizure_Date,11,2),':',substr(Channel_Seizure_Date,13,2)), NULL) as channel_seizure_time,
trim(Call_Action_Code),
trim(Unit_ESN),
trim(Ring_Time),
trim(At_Call_Dur_Sec),
trim(Call_Term_Code),
trim(Call_To_Tn),
trim(Orig_Cell_Trunk_Id),
trim(Term_Cell_Trunk_Id),
trim(Serv_Sid),
trim(Home_Sid),
trim(Mps_File_Number),
trim(Toll_Type),
trim(Market_Code),
trim(Message_Type),
trim(Call_Direction_Code),
trim(Characteristics),
substr(trim(Error_Codes),6,length(trim(Error_Codes))) as additional_error_codes,
trim(Struct_Code),
trim(Call_Type),
trim(Units_Uom),
trim(Message_Switch_Id),
cast(substr(trim(Error_Codes),1,4) as int) as unbillable_reason_code,
'P' as format_code,
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
concat(substr(Channel_Seizure_Date, 1, 4 ),'-',substr( Channel_Seizure_Date, 5, 2 ),'-',substr( Channel_Seizure_Date, 7, 2 )) as channel_seizure_date
FROM cdrdm.glenayredrop a ${hiveconf:WHERE_clause};