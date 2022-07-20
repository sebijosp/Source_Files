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

INSERT INTO TABLE cdrdm.FACT_IMS_CDR PARTITION (End_Date)
SELECT
ServedPartyNumber as Served_Party_Number, --cast(bigint)
OtherPartyNumber as Other_Party_Number, --cast(bigint)
CONCAT(SUBSTR(StartDateTime,1,4),'-',SUBSTR(StartDateTime,5,2),'-',SUBSTR(StartDateTime,7,2),' ',SUBSTR(StartDateTime,9,2),':',SUBSTR(StartDateTime,11,2),':',SUBSTR(StartDateTime,13,2)) as Start_Date_Time,
CONCAT(SUBSTR(AnsweredDateTime,1,4),'-',SUBSTR(AnsweredDateTime,5,2),'-',SUBSTR(AnsweredDateTime,7,2),' ',SUBSTR(AnsweredDateTime,9,2),':',SUBSTR(AnsweredDateTime,11,2),':',SUBSTR(AnsweredDateTime,13,2)) as Answered_Date_Time,
CONCAT(SUBSTR(ReleaseDateTime,1,4),'-',SUBSTR(ReleaseDateTime,5,2),'-',SUBSTR(ReleaseDateTime,7,2),' ',SUBSTR(ReleaseDateTime,9,2),':',SUBSTR(ReleaseDateTime,11,2),':',SUBSTR(ReleaseDateTime,13,2)) as End_Date_Time,
--CONCAT(substr(lpad(cast(cast((ReleaseDateTime - AnsweredDateTime) as int) as string), 5, '0'),1,1),':', substr(lpad(cast(cast((ReleaseDateTime - AnsweredDateTime) as int) as string), 5, '0'),2,2),':', substr(lpad(cast(cast((ReleaseDateTime - AnsweredDateTime) as int) as string), 5, '0'),4,2)) as Duration,
concat(hour(from_unixtime(unix_timestamp(CONCAT(SUBSTR(ReleaseDateTime,1,4),'-',SUBSTR(ReleaseDateTime,5,2),'-',SUBSTR(ReleaseDateTime,7,2),' ',SUBSTR(ReleaseDateTime,9,2),':',SUBSTR(ReleaseDateTime,11,2),':',SUBSTR(ReleaseDateTime,13,2))) - unix_timestamp(CONCAT(SUBSTR(AnsweredDateTime,1,4),'-',SUBSTR(AnsweredDateTime,5,2),'-',SUBSTR(AnsweredDateTime,7,2),' ',SUBSTR(AnsweredDateTime,9,2),':',SUBSTR(AnsweredDateTime,11,2),':',SUBSTR(AnsweredDateTime,13,2)))))-19,':',lpad(minute(from_unixtime(unix_timestamp(CONCAT(SUBSTR(ReleaseDateTime,1,4),'-',SUBSTR(ReleaseDateTime,5,2),'-',SUBSTR(ReleaseDateTime,7,2),' ',SUBSTR(ReleaseDateTime,9,2),':',SUBSTR(ReleaseDateTime,11,2),':',SUBSTR(ReleaseDateTime,13,2))) - unix_timestamp(CONCAT(SUBSTR(AnsweredDateTime,1,4),'-',SUBSTR(AnsweredDateTime,5,2),'-',SUBSTR(AnsweredDateTime,7,2),' ',SUBSTR(AnsweredDateTime,9,2),':',SUBSTR(AnsweredDateTime,11,2),':',SUBSTR(AnsweredDateTime,13,2))))),2,'0'),':',lpad(second(from_unixtime(unix_timestamp(CONCAT(SUBSTR(ReleaseDateTime,1,4),'-',SUBSTR(ReleaseDateTime,5,2),'-',SUBSTR(ReleaseDateTime,7,2),' ',SUBSTR(ReleaseDateTime,9,2),':',SUBSTR(ReleaseDateTime,11,2),':',SUBSTR(ReleaseDateTime,13,2))) - unix_timestamp(CONCAT(SUBSTR(AnsweredDateTime,1,4),'-',SUBSTR(AnsweredDateTime,5,2),'-',SUBSTR(AnsweredDateTime,7,2),' ',SUBSTR(AnsweredDateTime,9,2),':',SUBSTR(AnsweredDateTime,11,2),':',SUBSTR(AnsweredDateTime,13,2))))),2,'0')),
MessageSwitchID as Switch_ID,
CASE WHEN cast(CallActionCode as int) = 1 THEN 'Incoming' WHEN cast(CallActionCode as int) = 2 THEN 'Outgoing' ELSE cast(CallActionCode as int) END as Call_Code,
CallTerminationCode as Termination_Code, --cast(int)
ServiceType as Service_Type,
UtcOffset as Time_Zone,	
TariffClass as Tariff_Class,
AccessType as Access_Type,
PresentationIndicator as Presentation_Indicator,
ServedPartyDeviceID as Device_ID,
UniqueSessionID as Session_ID,
UniqueRecordID as Record_ID,
PartialCallIndicator as Partial_Call_Indicator,
CellIdentifier as Cell_ID,
CallType as Call_Type,
CallPullFlag as Call_Pull_Flag,
DialedDigits as Dialed_Digits,
TeleserviceCode as TeleService_Code,
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
CONCAT(SUBSTR(ReleaseDateTime,1,4),'-',SUBSTR(ReleaseDateTime,5,2),'-',SUBSTR(ReleaseDateTime,7,2)) as End_Date
FROM cdrdm.ims1205ascii a ${hiveconf:WHERE_clause};