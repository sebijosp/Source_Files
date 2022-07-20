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

set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx8500m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx8500m;

SET WHERE_clause;

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
cast(record_Type_ind as int),
served_IMSI,
ggsn_Address,
charging_ID,
sgsn_Address_1,
sgsn_Address_2,
sgsn_Address_3,
sgsn_Address_4,
sgsn_Address_5,
access_Point_Name_NI,
pdp_Type,
served_PDP_Address,
dynamic_Address_Flag,
qos_Negotiated_1,
cast(data_Volume_GPRS_Uplink_1 as bigint),
cast(data_Volume_GPRS_Downlink_1 as bigint),
change_Condition_1,
if((change_Time_1 is not NULL) and length(trim(change_Time_1)) > 0, concat(substr(change_Time_1,1,8),' ',substr(change_Time_1,10,2),':',substr(change_Time_1,13,2),':',substr(change_Time_1,16,2)), NULL),
NULL,
user_Location_Information_1,
NULL,
qos_Negotiated_2,
cast(data_Volume_GPRS_Uplink_2 as bigint),
cast(data_Volume_GPRS_Downlink_2 as bigint),
change_Condition_2,
if((change_Time_2 is not NULL) and length(trim(change_Time_2)) > 0, concat(substr(change_Time_2,1,8),' ',substr(change_Time_2,10,2),':',substr(change_Time_2,13,2),':',substr(change_Time_2,16,2)), NULL),
NULL,
user_Location_Information_2,
NULL,
qos_Negotiated_3,
cast(data_Volume_GPRS_Uplink_3 as bigint),
cast(data_Volume_GPRS_Downlink_3 as bigint),
change_Condition_3,
if((change_Time_3 is not NULL) and length(trim(change_Time_3)) > 0, concat(substr(change_Time_3,1,8),' ',substr(change_Time_3,10,2),':',substr(change_Time_3,13,2),':',substr(change_Time_3,16,2)), NULL),
NULL,
user_Location_Information_3,
NULL,
qos_Negotiated_4,
cast(data_Volume_GPRS_Uplink_4 as bigint),
cast(data_Volume_GPRS_Downlink_4 as bigint),
change_Condition_4,
if((change_Time_4 is not NULL) and length(trim(change_Time_4)) > 0, concat(substr(change_Time_4,1,8),' ',substr(change_Time_4,10,2),':',substr(change_Time_4,13,2),':',substr(change_Time_4,16,2)), NULL),
NULL,
user_Location_Information_4,
NULL,
qos_Negotiated_5,
cast(data_Volume_GPRS_Uplink_5 as bigint),
cast(data_Volume_GPRS_Downlink_5 as bigint),
change_Condition_5,
if((change_Time_5 is not NULL) and length(trim(change_Time_5)) > 0, concat(substr(change_Time_5,1,8),' ',substr(change_Time_5,10,2),':',substr(change_Time_5,13,2),':',substr(change_Time_5,16,2)), NULL),
NULL,
user_Location_Information_5,
NULL,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, concat(substr(record_Opening_Time,1,8),' ',substr(record_Opening_Time,10,2),':',substr(record_Opening_Time,13,2),':',substr(record_Opening_Time,16,2)), 0),
NULL,
cast(duration as int),
cast(cause_For_Rec_Closing as int),
cast(record_Sequence_Number as bigint),
node_ID,
cast(local_Sequence_Number as bigint),
cast(apn_Selection_Mode as int),
served_MSISDN,
cast(substr(trim(served_MSISDN),4,10) as bigint) as subscriber_no,
NULL,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
cast(rAT_Type as int),
mS_Time_Zone,
user_Location_Information,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
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
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, concat('20', substr(record_Opening_Time,1,8)),NULL) as record_opening_date
FROM cdrdm.mpgggsn a ${hiveconf:WHERE_clause};
