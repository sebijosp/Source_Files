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



INSERT INTO TABLE cdrdm.FACT_RHP_MAESTRO PARTITION (field_date)
SELECT
CONCAT(
case when substr(orig_NPA, 1,1) not between '0' and '9' then '' else substr(orig_NPA, 1,1) end,
case when substr(orig_NPA, 2,1) not between '0' and '9' then '' else substr(orig_NPA, 2,1) end,
case when substr(orig_NPA, 3,1) not between '0' and '9' then '' else substr(orig_NPA, 3,1) end,
case when substr(orig_number, 1,1) not between '0' and '9' then '' else substr(orig_number, 1,1) end,
case when substr(orig_number, 2,1) not between '0' and '9' then '' else substr(orig_number, 2,1) end,
case when substr(orig_number, 3,1) not between '0' and '9' then '' else substr(orig_number, 3,1) end,
case when substr(orig_number, 4,1) not between '0' and '9' then '' else substr(orig_number, 4,1) end,
case when substr(orig_number, 5,1) not between '0' and '9' then '' else substr(orig_number, 5,1) end,
case when substr(orig_number, 6,1) not between '0' and '9' then '' else substr(orig_number, 6,1) end,
case when substr(orig_number, 7,1) not between '0' and '9' then '' else substr(orig_number, 7,1) end,
case when substr(orig_number, 8,1) not between '0' and '9' then '' else substr(orig_number, 8,1) end,
case when substr(orig_number, 9,1) not between '0' and '9' then '' else substr(orig_number, 9,1) end,
case when substr(orig_number,10,1) not between '0' and '9' then '' else substr(orig_number,10,1) end) as subscriber_no,
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
NULL,
NULL,
answer_ind,
NULL,
NULL,
service_feature,
CONCAT(case when substr(orig_NPA, 1,1) not between '0' and '9' then '' else substr(orig_NPA, 1,1) end,
case when substr(orig_NPA, 2,1) not between '0' and '9' then '' else substr(orig_NPA, 2,1) end,
case when substr(orig_NPA, 3,1) not between '0' and '9' then '' else substr(orig_NPA, 3,1) end,
case when substr(orig_number, 1,1) not between '0' and '9' then '' else substr(orig_number, 1,1) end,
case when substr(orig_number, 2,1) not between '0' and '9' then '' else substr(orig_number, 2,1) end,
case when substr(orig_number, 3,1) not between '0' and '9' then '' else substr(orig_number, 3,1) end,
case when substr(orig_number, 4,1) not between '0' and '9' then '' else substr(orig_number, 4,1) end,
case when substr(orig_number, 5,1) not between '0' and '9' then '' else substr(orig_number, 5,1) end,
case when substr(orig_number, 6,1) not between '0' and '9' then '' else substr(orig_number, 6,1) end,
case when substr(orig_number, 7,1) not between '0' and '9' then '' else substr(orig_number, 7,1) end,
case when substr(orig_number, 8,1) not between '0' and '9' then '' else substr(orig_number, 8,1) end,
case when substr(orig_number, 9,1) not between '0' and '9' then '' else substr(orig_number, 9,1) end,
case when substr(orig_number,10,1) not between '0' and '9' then '' else substr(orig_number,10,1) end) as orig_number,
overseas_ind,
CONCAT(cast(cast(term_NPA as char(5)) as int), cast(term_Number as char(7))) as term_number,
cast(CONCAT(substr(answer_time,1,2),':',substr(answer_time,3,2),':',substr(answer_time,5,2)) as STRING),
cast(elapsed_Time as char(9)), --cast(int)
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
case when (length(trim(mod022_present_Date) ) > 0) then cast (CONCAT(substr(cast(year(from_unixtime(unix_timestamp())) as char(4)),1,3),substr (mod022_present_Date,1,1),'-',substr (mod022_present_Date,2,2),'-',substr (mod022_present_Date,4,2),' ',substr (mod022_present_Time,1,2),':',substr (mod022_present_Time,3,2),':',substr (mod022_present_Time,5,2)) as STRING) end as mod022_present_datetime,
case when (length(trim(mod025_release_Date)) > 0) then cast (CONCAT(substr(cast(year(from_unixtime(unix_timestamp())) as char(4)),1,3),substr (mod025_release_Date,1,1),'-',substr (mod025_release_Date,2,2),'-',substr (mod025_release_Date,4,2),' ',substr (mod025_release_Time,1,2),':',substr (mod025_release_Time,3,2),':',substr (mod025_release_Time,5,2)) as STRING) end as mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
case when (length(trim(mod125_alerting_Date) ) > 0) then cast (CONCAT(substr(cast(year(from_unixtime(unix_timestamp())) as char(4)),1,3),substr (mod125_alerting_Date,1,1),'-',substr (mod125_alerting_Date,2,2),'-',substr (mod125_alerting_Date,4,2),' ',substr (mod125_alerting_Time,1,2),':',substr (mod125_alerting_Time,3,2),':',substr (mod125_alerting_Time,5,2)) as STRING) end as mod125_alerting_datetime,
cast(cast(mod125_elapsed_Time as char(9)) as int),
mod125_party_identifier,
mod125_completion_ind,
mod338_1_party_identifier,
mod338_1_type_of_service,
mod338_1_serv_provider_id,
mod338_2_party_identifier,
mod338_2_type_of_service,
mod338_2_serv_provider_id,
mod720_party_identifier,
mod720_location_rout_number,
mod720_serv_provider_ident,
mod720_location,
mod720_supporting_info,
NEW_FIELD2,
NULL,
NULL,
UNIQUECALLID,
file_name,
record_start,
record_end,
record_type,
'dpsamasc0500', --family_name
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()),
CONCAT(SUBSTR(CONCAT('2',(substr(('0100000'),1,7 - length(trim(cast(field_date as char(5)))))),trim(cast(field_date as char(5)))),1,4),'-',SUBSTR(CONCAT('2',(substr(('0100000'),1,7 - length(trim (cast(field_date as char(5)))))),trim(cast(field_date as char(5)))),5,2),'-',SUBSTR(CONCAT('2',(substr(('0100000'),1,7 - length(trim (cast(field_date as char(5)))))),trim(cast(field_date as char(5)))),7,2)) as field_date
FROM cdrdm.dpsamasc0500 a ${hiveconf:WHERE_clause}
AND
(CONCAT(
case when substr(a.orig_NPA, 1,1) not between '0' and '9' then '' else substr(a.orig_NPA, 1,1) end,
case when substr(a.orig_NPA, 2,1) not between '0' and '9' then '' else substr(a.orig_NPA, 2,1) end,
case when substr(a.orig_NPA, 3,1) not between '0' and '9' then '' else substr(a.orig_NPA, 3,1) end,
case when substr(a.orig_number, 1,1) not between '0' and '9' then '' else substr(a.orig_number, 1,1) end,
case when substr(a.orig_number, 2,1) not between '0' and '9' then '' else substr(a.orig_number, 2,1) end,
case when substr(a.orig_number, 3,1) not between '0' and '9' then '' else substr(a.orig_number, 3,1) end,
case when substr(a.orig_number, 4,1) not between '0' and '9' then '' else substr(a.orig_number, 4,1) end,
case when substr(a.orig_number, 5,1) not between '0' and '9' then '' else substr(a.orig_number, 5,1) end,
case when substr(a.orig_number, 6,1) not between '0' and '9' then '' else substr(a.orig_number, 6,1) end,
case when substr(a.orig_number, 7,1) not between '0' and '9' then '' else substr(a.orig_number, 7,1) end,
case when substr(a.orig_number, 8,1) not between '0' and '9' then '' else substr(a.orig_number, 8,1) end,
case when substr(a.orig_number, 9,1) not between '0' and '9' then '' else substr(a.orig_number, 9,1) end,
case when substr(a.orig_number,10,1) not between '0' and '9' then '' else substr(a.orig_number,10,1) end) 
not in (SELECT b.subscriber_no from ela_v21.subscriber b)
and a.file_name like '%_sf.dat') ;
