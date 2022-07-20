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
SET hive.exec.max.dynamic.partitions=20000;

MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_1;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_10;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_11;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_12;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_13;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_14;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_15;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_16;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_17;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_18;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_19;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_2;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_20;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_21;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_22;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_23;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_24;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_25;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_26;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_27;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_28;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_29;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_3;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_30;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_31;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_32;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_4;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_5;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_6;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_7;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_8;
MSCK REPAIR TABLE ext_cdrdm.FACT_GSM_CDR_9;

CREATE TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST(
subscriber_no BIGINT,
record_sequence_number BIGINT,
call_type SMALLINT,
imsi BIGINT,
imei BIGINT,
exchange_identity CHAR(15),
switch_identity CHAR(4),
call_timestamp STRING,
chargeable_duration INT,
calling_party_number VARCHAR(30),
cleansed_calling_number BIGINT,
called_party_number VARCHAR(30),
cleansed_called_number BIGINT,
original_called_number VARCHAR(30),
cleansed_original_number BIGINT,
reg_seizure_charging_start INT,
mobile_station_roaming_number BIGINT,
redirecting_number VARCHAR(30),
cleansed_redirecting_number BIGINT,
fault_code CHAR(4),
--switch_id CHAR(8),
eos_info BIGINT,
--time_key INT,
incoming_route_id VARCHAR(7),
outgoing_route_id VARCHAR(7),
first_cell_id CHAR(14),
last_cell_id CHAR(14),
charged_party SMALLINT,
charged_party_number VARCHAR(30),
cleansed_charged_number BIGINT,
internal_cause_and_loc CHAR(5),
traffic_activity_code VARCHAR(8),
disconnecting_party INT,
partial_output_num SMALLINT,
rco CHAR(2),
--audit_key INT,
ocn CHAR(4),
multimediacall SMALLINT,
teleservicecode SMALLINT,
tariffclass SMALLINT,
First_Cell_ID_Extension VARCHAR(4),
subscriptionType CHAR(4),
SRVCCIndicator CHAR(2),
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
PARTITIONED BY (call_timestamp_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY");

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.Z_FACT_GSM_CDR_DEDUP_HIST PARTITION (call_timestamp_date)
SELECT
trim(subscriber_no),
trim(record_sequence_number),
trim(call_type),
trim(imsi),
trim(imei),
exchange_identity,
switch_identity,
call_timestamp,
trim(chargeable_duration),
calling_party_number,
trim(cleansed_calling_number),
called_party_number,
trim(cleansed_called_number),
original_called_number,
trim(cleansed_original_number),
trim(reg_seizure_charging_start),
trim(mobile_station_roaming_number),
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
trim(partial_output_num),
rco,
--trim(audit_key),
ocn,
multimediacall,
trim(teleservicecode),
trim(tariffclass),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
substr(call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.FACT_GSM_CDR_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD