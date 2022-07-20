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
SET hive.tez.log.level=DEBUG;
SET hive.exec.max.dynamic.partitions=20000;

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_CDR PARTITION (call_timestamp_date)
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

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
SELECT
trim(record_sequence_number),
trim(imsi),
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
redirecting_number,
trim(cleansed_redirecting_number),
fault_code,
--switch_id,
trim(eos_info),
--trim(time_key),
incoming_route_id,
outgoing_route_id,
trim(charged_party),
charged_party_number,
trim(cleansed_charged_number),
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
--trim(audit_key),
subscriptionType,
'FACT_GSM_TRANSIT_CDR', --file_name
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
FROM ext_cdrdm.FACT_GSM_TRANSIT_CDR_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE_PP PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
--channel_seizure_date,
channel_seizure_time,
call_action_code,
toll_type,
message_type,
call_type,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
time_for_start_of_charge,
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
unbillable_reason_code,
--trim(audit_key),
--trim(time_key),
First_Cell_ID_Extension,
subscriptionType,
SRVCCIndicator,
'FACT_GSM_UNBILLABLE_PP', --file_name
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
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE_PP_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
trim(channel_seizure_time),
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
call_data_module_choice_mask,
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
trim(chargeable_duration),
calling_party_number,
called_party_number,
original_called_number,
trim(reg_seizure_charging_start),
mobile_station_roaming_number,
redirecting_number,
--trim(time_key),
switch_id,
incoming_route_id,
outgoing_route_id,
first_cell_id,
trim(calling_reporting_centre_id),
trim(called_reporting_centre_id),
unbillable_reason_code,
trim(significant_number_key),
format_code,
--trim(eng_date_id),
--trim(audit_key),
subscription_type,
srvcc_indicator,
'FACT_GSM_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gsm1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GSM_UNBILLABLE a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
trim(subscriber_no),
trim(event_record_id),
trim(spid),
orig_msc,
trim(other_msisdn),
orig_msc_zone_indicator,
trim(rate),
charged_party,
action,
trim(rule),
trim(transaction_id),
trim(charge_result),
trim(balance),
trim(rate_plan),
on_net,
originating_service_grade,
terminating_service_grade,
trim(sequence_number),
local_subscriber_timestamp,
trim(imsi),
trim(ban),
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,
--trim(switch_id),
--trim(time_key),
--trim(audit_key),
ocn,
other_msisdn_sc,
TR_Type_OMSC,
TR_OMSC_PLMN,
TR_LAC_TAC,
TR_ECID_CLID,
TR_MSC_TYPE,
TR_DCODE_COUNTRY,
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
MTYPE,
'FACT_SMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'siumsmsraw', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
trim(switch_id), --switch_name
NULL, --num_records
day_date,
SUBSTR(local_subscriber_timestamp,1,10) as local_subscriber_date
FROM ext_cdrdm.FACT_SMS_CDR_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
channel_seizure_date as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
trim(ring_time),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
trim(call_data_module_choice_mask),
imsi,
imei,
exchange_identity,
switch_identity,
date_for_start_of_charge,
trim(time_for_start_of_charge),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
trim(time_from_rgstr_to_chrgn),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
--trim(audit_key),
--trim(time_key),
'FACT_IUM1205_SMS_DROP', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr (channel_seizure_date,1,10) as channel_seizure_date
FROM ext_cdrdm.FACT_IUM1205_SMS_DROP_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key <= TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
trim(record_sequence_number),
gprs_choice_mask_archive,
served_imsi,
--record_opening_date,
record_opening_time,
served_msisdn,
trim(data_volume_uplink_archive),
trim(data_volume_downlink_archive),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
--switch_id,
trim(reporting_centre_id),
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
trim(duration),
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
'FACT_GPRS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215ascii_gsm1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
record_opening_date
FROM ext_cdrdm.FACT_GPRS_CDR_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
channel_seizure_time,
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_dur_sec),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
routing_area,
location_area_code,
access_point_name,
--trim(time_key),
switch_id,
orig_route_id,
term_route_id,
trim(reporting_centre_id),
trim(unbillable_reason_code),
format_code,
--trim(eng_date_id),
--trim(audit_key),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
trim(event_count),
domain_1,
plmn_id,
cell_id,
sgsn_ip,
monum,
trim(charging_id),
trim(duration),
node_id,
tracking_area_code,
eutran_cellid,
'FACT_GPRS_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ium1215drop_gsm1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GPRS_UNBILLABLE_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_TAP3_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
subscriber_no,
--channel_seizure_date,
channel_seizure_time,
imsi,
trim(duration),
plmn_id,
sender,
recipent,
trim(mps_file_number),
drop_code,
pdp_addresses,
cell_id,
trim(downlink_volume),
trim(uplink_volume),
trim(file_sequence_number),
charging_id,
access_point_name,
--trim(audit_key),
--trim(time_key),
'FACT_TAP3_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'tap3drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_TAP3_UNBILLABLE a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_1;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_2;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_3;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_4;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_5;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_6;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_7;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_8;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_9;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_10;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_11;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_12;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_13;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_14;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_15;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_16;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_17;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_18;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_19;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_20;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_21;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_22;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_23;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_24;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_25;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_26;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_27;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_28;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_29;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_30;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_31;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
trim(record_Type),
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
trim(data_Volume_GPRS_Uplink_1),
trim(data_Volume_GPRS_Downlink_1),
change_Condition_1,
change_Time_1,
user_Location_Information_1,
qos_Negotiated_2,
trim(data_Volume_GPRS_Uplink_2),
trim(data_Volume_GPRS_Downlink_2),
change_Condition_2,
change_Time_2,
user_Location_Information_2,
qos_Negotiated_3,
trim(data_Volume_GPRS_Uplink_3),
trim(data_Volume_GPRS_Downlink_3),
change_Condition_3,
change_Time_3,
user_Location_Information_3,
qos_Negotiated_4,
trim(data_Volume_GPRS_Uplink_4),
trim(data_Volume_GPRS_Downlink_4),
change_Condition_4,
change_Time_4,
user_Location_Information_4,
qos_Negotiated_5,
trim(data_Volume_GPRS_Uplink_5),
trim(data_Volume_GPRS_Downlink_5),
change_Condition_5,
change_Time_5,
user_Location_Information_5,
record_Opening_Time,
trim(duration),
trim(cause_For_Rec_Closing),
trim(record_Sequence_Number),
node_ID,
trim(local_Sequence_Number),
trim(apn_Selection_Mode),
served_MSISDN,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
trim(rAT_Type),
mS_Time_Zone,
user_Location_Information,
trim(Subscriber_no),
--trim(Audit_Key),
--Insrt_TimeStamp,
'FACT_GPRS_MPG_GGSN_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'mpgggsn', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_TimeStamp,
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, substr(record_Opening_Time,1,10),NULL) as record_opening_date
FROM ext_cdrdm.FACT_GPRS_MPG_GGSN_CDR_32;
--WHERE Insrt_TimeStamp < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_1 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_2 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_3 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_4 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_5 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_6 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_7 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_8 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_9 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_10 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_11 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_12 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_13 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_14 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_15 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_16 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_17 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_18 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_19 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_20 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_21 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_22 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_23 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_24 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_25 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_26 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_27 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_28 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_29 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_30 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_31 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY PARTITION (field_date)
SELECT
trim(subscriber_no),
drop_code,
rec_des,
hex_id,
struct_code,
call_type,
sensor_type,
sensor_id,
rcrdoffice_type,
rcrdoffice_id,
--field_date,
timing_ind,
study_ind,
answer_ind,
serv_obsrvd_traf_sampled,
operator_action,
service_feature,
orig_number,
overseas_ind,
term_number,
answer_time,
trim(elapsed_time),
activating_number,
forwardto_overseas_ind,
forwardto_number,
activation_time,
trunk_legs_used,
deactivation_datetime,
ic_inc_prefix,
carrier_connect_datetime,
trim(carrier_elapsed_time),
ic_inc_call_event_status,
trunk_group_number,
routing_ind,
dialing_ind,
ani_ind,
class_feature_code,
farend_overseas_ind,
farend_number,
field_time,
class_functions,
class_feature_status,
screenlistsize_scf_sca,
screenlistsize_scr,
screenlistsize_dr_cw,
mod022_present_datetime,
mod025_release_datetime,
mod042_call_record_seq,
mod104_trunk_ident_number,
mod125_alerting_datetime,
trim(mod125_elapsed_time),
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
--switch_id,
--trim(time_key),
--trim(audit_key),
new_field2,
digits_dialed_1_eur,
digits_dialed_2_eur,
uniquecallid,
'FACT_RHPC_DPSCSV_VQ_DAILY', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'dpsama', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_id, --switch_name
NULL, --num_records
day_date,
field_date
FROM ext_cdrdm.FACT_RHPC_DPSCSV_VQ_DAILY_32 a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_BOBO_CDR PARTITION (transaction_date)
SELECT
transaction_id,
--transaction_date,
transaction_time,
trim(charged_party_msisdn),
trim(charged_party_imsi),
trim(charged_party_imei),
charged_party_spid,
transaction_type,
trim(direction),
original_transaction_id,
trim(partner_id),
partner_name,
trim(provider_id),
provider_name,
service_description,
billing_support_info,
application_support_info,
trim(total_amount),
charge_msdn_type,
user_id,
source_id,
trim(reason_code),
--trim(audit_key),
--switch_name,
--trim(time_key),
'FACT_BOBO_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'bobo', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
switch_name, --switch_name
NULL, --num_records
day_date,
transaction_date
FROM ext_cdrdm.FACT_BOBO_CDR a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_IMS_CDR PARTITION (End_Date)
SELECT
trim(Served_Party_Number),
trim(Other_Party_Number),
Start_Date_Time,
Answered_Date_Time,
Release_Date_Time,
CONCAT(substr(lpad(cast(cast((Release_Date_Time - Answered_Date_Time) as int) as string), 5, '0'),1,1),':', substr(lpad(cast(cast((Release_Date_Time - Answered_Date_Time) as int) as string), 5, '0'),2,2),':', substr(lpad(cast(cast((Release_Date_Time - Answered_Date_Time) as int) as string), 5, '0'),4,2)) as Duration,
Message_SwitchID,
CASE WHEN cast(Call_Action_Code as int) = 1 THEN 'Incoming' WHEN cast(Call_Action_Code as int) = 2 THEN 'Outgoing' ELSE cast(Call_Action_Code as int) END as Call_Code,
trim(Call_Termination_Code),
Service_Type,
UTC_Offset,
Tariff_Class,
Access_Type,
Presentation_Indicator,
Served_Party_DeviceID,
Unique_SessionID,
Unique_RecordID,
Partial_CallIndicator,
Cell_Identifier,
Call_Type,
Call_Pull_Flag,
Dialed_Digits,
TeleService_Code,
--trim(Audit_key),
--Time_key,
'FACT_IMS_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ims1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
SUBSTR(Release_Date_Time,1,10) as End_Date
FROM ext_cdrdm.FACT_IMS_CDR a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE Time_key < TBD

INSERT INTO TABLE cdrdm.FACT_IMS_DROP_CDR PARTITION (Start_Date)
SELECT
Drop_Code,
trim(Served_Party_Number),
Start_Date_Time,
Answered_Date_Time,
Release_Date_Time,
SwitchID,
trim(Call_Action_Code),
trim(Call_Termination_Code),
trim(Other_Party_Number),
Service_Type,
UTC_Offset,
Tariff_Class,
Access_Type,
Presentation_Indicator,
Unique_RecordID,
Served_Party_DeviceID,
Unique_SessionID,
Partial_CallIndicator,
TeleService_Code,
Cell_Identifier,
Call_Type,
Call_Pull_Flag,
Dialed_Digits,
--trim(Audit_key),
--Time_key,
'FACT_IMS_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'ims1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
SUBSTR(Start_Date_Time,1,10) as Start_Date
FROM ext_cdrdm.FACT_IMS_DROP_CDR a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE Time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GPRS_SGW_CDR PARTITION (recordOpeningDate)
SELECT
trim(recordType),
servedIMSI,
sGWAddressAddress,
chargingID,
servingNodeAddress1,
servingNodeAddress2,
servingNodeAddress3,
servingNodeAddress4,
servingNodeAddress5,
pdpPDNType,
servedPDPPDNAddress,
trim(dataVolumeGPRSUplink1),
trim(dataVolumeGPRSDownlink1),
changeCondition1,
changeTime1,
userLocationInformation1,
trim(qCI1),
trim(maxRequestedBandwithUL1),
trim(maxRequestedBandwithDL1),
trim(guaranteeBitrateUL1),
trim(guaranteeBitrateDL1),
trim(aRP1),
trim(dataVolumeGPRSUplink2),
trim(dataVolumeGPRSDownlink2),
changeCondition2,
changeTime2,
userLocationInformation2,
trim(qCI2),
trim(maxRequestedBandwithUL2),
trim(maxRequestedBandwithDL2),
trim(guaranteeBitrateUL2),
trim(guaranteeBitrateDL2),
trim(aRP2),
trim(dataVolumeGPRSUplink3),
trim(dataVolumeGPRSDownlink3),
changeCondition3,
changeTime3,
userLocationInformation3,
trim(qCI3),
trim(maxRequestedBandwithUL3),
trim(maxRequestedBandwithDL3),
trim(guaranteeBitrateUL3),
trim(guaranteeBitrateDL3),
trim(aRP3),
trim(dataVolumeGPRSUplink4),
trim(dataVolumeGPRSDownlink4),
changeCondition4,
changeTime4,
userLocationInformation4,
trim(qCI4),
trim(maxRequestedBandwithUL4),
trim(maxRequestedBandwithDL4),
trim(guaranteeBitrateUL4),
trim(guaranteeBitrateDL4),
trim(aRP4),
trim(dataVolumeGPRSUplink5),
trim(dataVolumeGPRSDownlink5),
changeCondition5,
changeTime5,
userLocationInformation5,
trim(qCI5),
trim(maxRequestedBandwithUL5),
trim(maxRequestedBandwithDL5),
trim(guaranteeBitrateUL5),
trim(guaranteeBitrateDL5),
trim(aRP5),
recordOpeningTime,
trim(duration),
trim(causeForRecClosing),
trim(recordSequenceNumber),
trim(localSequenceNumber),
servedMSISDN,
chargingCharacteristics,
servingNodePLMNIdentifier,
trim(rATType),
mSTimeZone,
sGWChange,
servingNodeType1,
servingNodeType2,
servingNodeType3,
servingNodeType4,
servingNodeType5,
pGWAddressUsed,
pGWPLMNIdentifier,
trim(pDNConnectionID),
APN,
Served_IMEI,
--trim(Audit_key),
--Insrt_Tmstmp,
ECID,
TAC_HEX,
ECID_HEX,
'FACT_GPRS_SGW_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'sgw1215ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_Tmstmp,
substr(recordOpeningTime,1,10) as recordOpeningDate
FROM ext_cdrdm.FACT_GPRS_SGW_CDR;
--WHERE Insrt_Tmstmp < TBD

INSERT INTO TABLE cdrdm.FACT_SGW_DROP_CDR PARTITION (record_opening_date)
SELECT
error_codes,
trim(gprs_choice_mask),
served_imsi,
record_opening_time,
served_msisdn,
trim(data_volume_gprs_uplink),
trim(data_volume_gprs_downlink),
tracking_area_code,
access_point_name,
enode,
charging_id,
trim(duration),
trim(cause_for_term),
pdp_ip_address,
pgw_ip_address,
sgw_ip_address,
switch_id,
served_imei,
--trim(Audit_Key),
--Insrt_Tmstmp,
'FACT_SGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'sgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
Insrt_Tmstmp,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_SGW_DROP_CDR;
--WHERE Insrt_Tmstmp < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_1;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_2;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_3;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_4;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_5;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_6;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_7;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_8;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_9;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_10;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_11;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_12;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_13;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_14;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_15;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_16;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_17;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_18;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_19;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_20;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_21;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_22;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_23;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_24;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_25;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_26;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_27;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_28;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_29;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_30;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_31;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_PGW_DROP_CDR PARTITION (record_opening_date)
SELECT
ERROR_CODES,
GPRS_CHOICE_MASK,
SERVED_IMSI,
RECORD_OPENING_TIME,
SERVED_MSISDN,
trim(DATA_VOLUME_GPRS_UPLINK),
trim(DATA_VOLUME_GPRS_DOWNLINK),
TRACKING_AREA_CODE,
ACCESS_POINT_NAME,
ENODE,
CHARGING_ID,
trim(DURATION),
trim(CAUSE_FOR_TERM),
PGW_IP_ADDRESS,
SGW_IP_ADDRESS,
SWITCH_ID,
SERVED_IMEI,
--trim(AUDIT_KEY),
--INSRT_TMSTMP,
'FACT_PGW_DROP_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'pgw1215drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
INSRT_TMSTMP,
substr(record_opening_time,1,10) as record_opening_date
FROM ext_cdrdm.FACT_PGW_DROP_CDR_32;
--WHERE INSRT_TMSTMP < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_1;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_2;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_3;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_4;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_5;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_6;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_7;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_8;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_9;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_10;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_11;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_12;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_13;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_14;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_15;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_16;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_17;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_18;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_19;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_20;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_21;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_22;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_23;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_24;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_25;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_26;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_27;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_28;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_29;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_30;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_31;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_CDR PARTITION (RecordOpeningDate)
SELECT
RecordType,
Retransmission,
SIPMethod,
RoleOfNode,
NodeAddress,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
SrvcRequestTimeStamp,
SrvcDeliveryStartTimeStamp,
SrvcDeliveryEndTimeStamp,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
SrvcDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(TmFromSipRqstStartOfChrgng),
InterOperatorIdentifiers,
LocalRecordSequenceNumber,
PartialOutputRecordNumber,
CauseForRecordClosing,
ACRStartLost,
ACRInterimLost,
ACRStopLost,
IMSILost,
ACRSCCASStartLost,
ACRSCCASInterimLost,
ACRSCCASStopLost,
IMSChargingIdentifier,
LstOfSDPMediaComponents,
LstOfNrmlMediaC1ChgTime,
LstOfNrmlMediaC1ChgTimeNor,
LstOfNrmlMediaC1DescType1,
LstOfNrmlMediaC1DescCodec1,
trim(LstOfNrmlMediaC1DescBndW1),
LstOfNrmlMediaC1DescType2,
LstOfNrmlMediaC1DescCodec2,
trim(LstOfNrmlMediaC1DescBndW2),
LstOfNrmlMediaC1DescType3,
LstOfNrmlMediaC1DescCodec3,
trim(LstOfNrmlMediaC1DescBndW3),
LstOfNrmlMediaC1medInitFlg,
LstOfNrmlMediaC2ChgTime,
LstOfNrmlMediaC2ChgTimeNor,
LstOfNrmlMediaC2DescType1,
LstOfNrmlMediaC2DescCodec1,
trim(LstOfNrmlMediaC2DescBndW1),
LstOfNrmlMediaC2DescType2,
LstOfNrmlMediaC2DescCodec2,
trim(LstOfNrmlMediaC2DescBndW2),
LstOfNrmlMediaC2DescType3,
LstOfNrmlMediaC2DescCodec3,
trim(LstOfNrmlMediaC2DescBndW3),
LstOfNrmlMediaC2medInitFlg,
LstOfNrmlMediaC3ChgTime,
LstOfNrmlMediaC3ChgTimeNor,
LstOfNrmlMediaC3DescType1,
LstOfNrmlMediaC3DescCodec1,
trim(LstOfNrmlMediaC3DescBndW1),
LstOfNrmlMediaC3DescType2,
LstOfNrmlMediaC3DescCodec2,
trim(LstOfNrmlMediaC3DescBndW2),
LstOfNrmlMediaC3DescType3,
LstOfNrmlMediaC3DescCodec3,
trim(LstOfNrmlMediaC3DescBndW3),
LstOfNrmlMediaC3medInitFlg,
LstOfNrmlMediaC4ChgTime,
LstOfNrmlMediaC4ChgTimeNor,
LstOfNrmlMediaC4DescType1,
LstOfNrmlMediaC4DescCodec1,
trim(LstOfNrmlMediaC4DescBndW1),
LstOfNrmlMediaC4DescType2,
LstOfNrmlMediaC4DescCodec2,
trim(LstOfNrmlMediaC4DescBndW2),
LstOfNrmlMediaC4DescType3,
LstOfNrmlMediaC4DescCodec3,
trim(LstOfNrmlMediaC4DescBndW3),
LstOfNrmlMediaC4medInitFlg,
LstOfNrmlMediaC5ChgTime,
LstOfNrmlMediaC5ChgTimeNor,
LstOfNrmlMediaC5DescType1,
LstOfNrmlMediaC5DescCodec1,
trim(LstOfNrmlMediaC5DescBndW1),
LstOfNrmlMediaC5DescType2,
LstOfNrmlMediaC5DescCodec2,
trim(LstOfNrmlMediaC5DescBndW2),
LstOfNrmlMediaC5DescType3,
LstOfNrmlMediaC5DescCodec3,
trim(LstOfNrmlMediaC5DescBndW3),
LstOfNrmlMediaC5medInitFlg,
LstOfNrmlMediaComponents6,
LstOfNrmlMediaComponents7,
LstOfNrmlMediaComponents8,
LstOfNrmlMediaComponents9,
LstOfNrmlMediaComponents10,
LstOfNrmlMediaComponents11,
LstOfNrmlMediaComponents12,
LstOfNrmlMediaComponents13,
LstOfNrmlMediaComponents14,
LstOfNrmlMediaComponents15,
LstOfNrmlMediaComponents16,
LstOfNrmlMediaComponents17,
LstOfNrmlMediaComponents18,
LstOfNrmlMediaComponents19,
LstOfNrmlMediaComponents20,
LstOfNrmlMediaComponents21,
LstOfNrmlMediaComponents22,
LstOfNrmlMediaComponents23,
LstOfNrmlMediaComponents24,
LstOfNrmlMediaComponents25,
LstOfNrmlMediaComponents26,
LstOfNrmlMediaComponents27,
LstOfNrmlMediaComponents28,
LstOfNrmlMediaComponents29,
LstOfNrmlMediaComponents30,
LstOfNrmlMediaComponents31,
LstOfNrmlMediaComponents32,
LstOfNrmlMediaComponents33,
LstOfNrmlMediaComponents34,
LstOfNrmlMediaComponents35,
LstOfNrmlMediaComponents36,
LstOfNrmlMediaComponents37,
LstOfNrmlMediaComponents38,
LstOfNrmlMediaComponents39,
LstOfNrmlMediaComponents40,
LstOfNrmlMediaComponents41,
LstOfNrmlMediaComponents42,
LstOfNrmlMediaComponents43,
LstOfNrmlMediaComponents44,
LstOfNrmlMediaComponents45,
LstOfNrmlMediaComponents46,
LstOfNrmlMediaComponents47,
LstOfNrmlMediaComponents48,
LstOfNrmlMediaComponents49,
LstOfNrmlMediaComponents50,
LstOfNrmlMedCompts1150,
GGSNAddress,
ServiceReasonReturnCode,
LstOfMessageBodies,
RecordExtension,
Expires,
Event,
Lst1AccessNetworkInfo,
Lst1AccessDomain,
Lst1AccessType,
Lst1LocationInfoType,
Lst1ChangeTime,
Lst1ChangeTimeNormalized,
Lst2AccessNetworkInfo,
Lst2AccessDomain,
Lst2AccessType,
Lst2LocationInfoType,
Lst2ChangeTime,
Lst2ChangeTimeNormalized,
Lst3AccessNetworkInfo,
Lst3AccessDomain,
Lst3AccessType,
Lst3LocationInfoType,
Lst3ChangeTime,
Lst3ChangeTimeNormalized,
Lst4AccessNetworkInfo,
Lst4AccessDomain,
Lst4AccessType,
Lst4LocationInfoType,
Lst4ChangeTime,
Lst4ChangeTimeNormalized,
Lst5AccessNetworkInfo,
Lst5AccessDomain,
Lst5AccessType,
Lst5LocationInfoType,
Lst5ChangeTime,
Lst5ChangeTimeNormalized,
Lst6AccessNetworkInfo,
Lst6AccessDomain,
Lst6AccessType,
Lst6LocationInfoType,
Lst6ChangeTime,
Lst6ChangeTimeNormalized,
Lst7AccessNetworkInfo,
Lst7AccessDomain,
Lst7AccessType,
Lst7LocationInfoType,
Lst7ChangeTime,
Lst7ChangeTimeNormalized,
Lst8AccessNetworkInfo,
Lst8AccessDomain,
Lst8AccessType,
Lst8LocationInfoType,
Lst8ChangeTime,
Lst8ChangeTimeNormalized,
Lst9AccessNetworkInfo,
Lst9AccessDomain,
Lst9AccessType,
Lst9LocationInfoType,
Lst9ChangeTime,
Lst9ChangeTimeNormalized,
Lst10AccessNetworkInfo,
Lst10AccessDomain,
Lst10AccessType,
Lst10LocationInfoType,
Lst10ChangeTime,
Lst10ChangeTimeNormalized,
ServiceContextID,
SubscriberE164,
trim(SubscriberNo),
IMSI,
IMEI,
SubSIPURI,
NAI,
SubPrivate,
SubServedPartyDeviceID,
LstOfEarlySDPMediaCmpnts,
IMSCommServiceIdentifier,
NumberPortabilityRouting,
CarrierSelectRouting,
SessionPriority,
RequestedPartyAddress,
LstOfCalledAssertedID,
MMTelInfoAnalyzedCallType,
MTlInfoCalledAsrtIDPrsntSts,
MTlInfoCllgPrtyAddrPrsntSts,
MMTelInfoConferenceId,
MMTelInfoDialAroundIndctr,
MMTelInfoRelatedICID,
MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo1Act,
MMTelSplmtrySrvcInfo1Redir,
MMTelSplmtrySrvcInfo2ID,
MMTelSplmtrySrvcInfo2Act,
MMTelSplmtrySrvcInfo2Redir,
MMTelInfoSplmtrySrvcInfo3,
MMTelInfoSplmtrySrvcInfo4,
MMTelInfoSplmtrySrvcInfo5,
MMTelInfoSplmtrySrvcInfo6,
MMTelInfoSplmtrySrvcInfo7,
MMTelInfoSplmtrySrvcInfo8,
MMTelInfoSplmtrySrvcInfo9,
MMTelInfoSplmtrySrvcInfo10,
MobileStationRoamingNumber,
TeleServiceCode,
TariffClass,
pVisitedNetworkID,
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
trim(TR_BAN),
TR_ACCOUNT_SUB_TYPE,
TR_COMPANY_CODE,
TR_FRANCHISE_CD,
TR_BA_ACCOUNT_TYPE,
'FACT_IMM_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205ascii', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_CDR_32;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_IMM_UNBILLABLE PARTITION (RecordOpeningDate)
SELECT
SIPMethod,
RoleOfNode,
SessionId,
CallingPartyAddress,
CalledPartyAddress,
RecordOpeningTime,
RecordClosureTime,
SrvcRequestDttmNrml,
SrvcDeliveryStartDttmNrml,
serviceDeliveryEndDttmNrml,
trim(ChargeableDuration),
trim(tmFromSipRqstStartOfChrgng),
PartialOutputRecordNumber,
IncompleteCDRIndications,
IMSChargingIdentifier,
AcessNetworkInfo1,
AccessDomain1,
AccessType1,
CTN,
SubIMSI,
SubIMEI,
RequestedPartyAddress,
MobileStationRoamingNumber,
TariffClass,
ErrCode,
trim(SubscriberNo),
--trim(AuditKey),
--trim(WrkflwCrtRunId),
--EtlCrtTs,
'FACT_IMM_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'imm1205drop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
EtlCrtTs,
if (RecordOpeningTime IS NOT NULL, substr(RecordOpeningTime,1,10),substr(EtlCrtTs,1,10)) as RecordOpeningDate
FROM ext_cdrdm.FACT_IMM_UNBILLABLE;
--WHERE EtlCrtTs < TBD

INSERT INTO TABLE cdrdm.FACT_RHPS_LD PARTITION (connect_date)
SELECT
trim(subscriber_no),
switch_id,
dc_baby_file,
trim(record_seq_number),
orig_country,
orig_line_number,
orig_extension,
orig_trunk_grp,
orig_trunk_number,
orig_city,
orig_province,
gmt_date,
term_country,
term_line_number,
term_extension,
term_trunk_grp,
term_trunk_number,
term_city,
term_province,
type_of_call,
answer_sup_type,
answer_sup_flag,
talk_time_flag,
network_class,
transmission_type,
off_premise_call,
rating_tier,
peak_hour_rating,
rate_table_code,
rate_flag,
trim(disc_or_per_min),
trim(telco_charge_tax_in),
trim(cust_charge_tax_in),
trim(cost_of_call),
customer_type,
product_type_1,
rating_datetime,
gst_exemption_flag,
pst_applicable_flag,
trim(telco_gst_amount),
trim(telco_pst_amount),
trim(customer_gst_amount),
trim(customer_pst_amount),
trim(minimum_charge),
trim(surcharge),
trim(surcharge_amount),
tax_type,
company_code,
office_id,
account_number,
circuit_id,
billing_number,
gmt_flag,
anb_destination,
onnet_city_id,
service_type,
billing_pool,
gmt_time,
originating_pin,
trim(pin_a),
trim(pin_b),
pin_a_length,
pin_b_length,
pin_a_description,
pin_b_description,
pin_lookup_code,
charge_code_flag,
pin_sort_type,
connect_datetime,
trim(air_seconds),
trim(talk_seconds),
trim(billable_seconds),
trim(other_seconds),
trim(telco_seconds),
trim(min_billable_time),
day_of_week_code,
telco_time_of_day,
company_time_of_day,
access_type,
trim(report_type),
call_group,
report_flag_1,
report_flag_2,
report_flag_3,
report_flag_4,
report_flag_5,
report_flag_6,
report_flag_7,
report_flag_8,
report_flag_9,
report_flag_10,
report_flag_11,
report_flag_12,
report_flag_13,
report_flag_14,
report_flag_15,
report_flag_16,
report_flag_17,
report_flag_18,
report_flag_19,
report_flag_20,
omni_zone,
cust_confidentiality_flag,
language_code_1,
country_region,
orig_telecard_country,
distribution_code,
opart_number,
misc_code_3_carrier_code,
misc_code_4,
misc_code_5,
trim(error_code),
trim(error_count),
trim(miles),
rating_type,
product_type_2,
is_telco_calculated,
trim(spec_cust_charge),
trim(spec_promo_disc),
trim(spec_rate),
trim(spec_bill_sec),
alt_prod_group,
alt_prod_type,
trim(alt_min_charge),
trim(alt_surcharge),
trim(alt_cust_charge),
trim(alt_telco_chg),
trim(alt_promo_disc),
alt_promo_code,
alt_tod,
trim(alt_rate),
alt_rate_flag,
trim(alt_bill_sec),
trim(alt_min_bill_sec),
trim(alt_spec_cust_charge),
trim(alt_spec_promo_disc),
trim(alt_spec_rate),
trim(alt_spec_bill_sec),
lrn,
operator_type,
sprint_rated,
sprint_billed,
retrieved_listing_number,
trim(vru_time),
trim(enhanced_toll_free_charges),
cicc_code,
billing_rao,
message_type,
current_data_source,
gst_province,
pst_province,
tax_code_3,
tax_province_3,
trim(tax_amount_3),
mill_3,
tax_code_4,
tax_province_4,
trim(tax_amount_4),
mill_4,
tax_code_5,
tax_province_5,
trim(tax_amount_5),
mill_5,
trim(total_tax_amt),
trim(additional_service_number),
trim(expanded_level_code_a),
trim(expanded_level_code_b),
trim(expanded_level_code_c),
report_flag_21,
report_flag_22,
report_flag_23,
report_flag_24,
report_flag_25,
report_flag_26,
report_flag_27,
report_flag_28,
report_flag_29,
report_flag_30,
report_flag_31,
report_flag_32,
report_flag_33,
report_flag_34,
report_flag_35,
report_flag_36,
report_flag_37,
report_flag_38,
report_flag_39,
report_flag_40,
true_country_code,
international_city_code,
surcharge_code_1,
trim(surcharge_amt_1),
surcharge_code_2,
trim(surcharge_amt_2),
surcharge_code_3,
trim(surcharge_amt_3),
surcharge_code_4,
trim(surcharge_amt_4),
surcharge_code_5,
trim(surcharge_amt_5),
carrier_code,
service_area,
min_charge,
trim(min_charge_amt),
trim(base_rate),
trim(addn_rate),
trim(discount_percent),
account_code_raw,
ani_raw,
billing_number_raw,
called_number_raw,
dialled_number_raw,
comp_code_raw,
called_pre_digits_raw,
dialled_pre_digits_raw,
original_opart_raw,
original_country_raw,
operator_services_raw,
universal_access_raw,
pin_raw,
treatement_code_raw,
conference_call_raw,
originating_partition_raw,
rel_lite_trunk_call,
additional_serv_seq_num,
resi_actual_account_number,
billing_type,
payment_method,
feature_id,
market_segment,
billing_option,
discount_code,
taxation_province,
origination_location,
conf_bridging_call_flag,
us_to_us_fon_call_flag,
disabled_customer_flag,
long_dist_cust_flag,
trim(discount_amount),
trim(discounted_call_charge),
accrual_type,
billing_date,
billed_traffic_type,
alternate_traffic_type,
language_code_2,
abs_report_category,
from_rao,
host_rao,
--trim(time_key),
--trim(audit_key),
'FACT_RHPS_LD', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'rtild', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr(connect_datetime,1,10) as connect_date
FROM ext_cdrdm.FACT_RHPS_LD a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GMPC_CDR PARTITION (position_date)
SELECT
trim(trafficcase),
trim(locationtype),
clientid,
clientno,
pushurl,
pushid,
trim(errorcode1),
trim(clienttype),
trim(privacyoverride),
trim(coordinatesystem),
trim(datum),
trim(requestedaccuracy),
trim(requestedaccuracymeters),
trim(responsetime),
requestedpositiontime,
subclientno,
clientrequestor,
trim(clientservicetype),
targetms,
numbermsc,
numbersgsn,
positiontime,
trim(levelofconfidence),
trim(obtainedaccuracy),
trim(errorcode2),
dialledbyms,
numbervlr,
trim(requestbearer),
setid,
latitude_v,
longitude_v,
trim(latitude),
trim(longitude),
trim(province_id),
trim(network),
trim(usedlocationmethod),
--trim(time_key),
--trim(audit_key),
esrd,
NumberMME,
trim(CallDuration),
'FACT_GMPC_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'gmpc', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
substr(positiontime,1,10) as position_date
FROM ext_cdrdm.FACT_GMPC_CDR a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GLENAYRE_CDR PARTITION (file_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
record_type,
access_number,
collection_time,
return_code,
service_type,
encoder_type,
capcode,
trim(coverage_region),
trim(call_count),
trim(character_count),
trim(vm_storage_seconds),
trim(meetme_connect_seconds),
customer_enabled,
customer_absent,
--trim(time_key),
switch_id,
--trim(engineering_date_id),
--trim(audit_key),
'FACT_GLENAYRE_CDR', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'glenayre', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date as insert_timestamp,
day_date as file_date
FROM ext_cdrdm.FACT_GLENAYRE_CDR a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.FACT_GLENAYRE_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(subscriber_no),
subscriber_no_char,
--channel_seizure_date,
trim(channel_seizure_time),
call_action_code,
unit_esn,
trim(ring_time),
trim(at_call_duration),
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
home_sid,
trim(mps_file_number),
toll_type,
market_code,
message_type,
call_direction_code,
characteristics_1,
additional_error_codes,
struct_cd,
call_type,
units_uom,
--trim(time_key),
switch_id,
trim(unbillable_reason_code),
format_code,
--trim(engineering_date_id),
--trim(audit_key),
'FACT_GLENAYRE_UNBILLABLE', --file_name
NULL, --record_start
NULL, --record_end
NULL, --record_type
'glenayredrop', --family_name
NULL, --version_id
NULL, --file_ts
NULL, --file_id
NULL, --switch_name
NULL, --num_records
day_date,
channel_seizure_date
FROM ext_cdrdm.FACT_GLENAYRE_UNBILLABLE a LEFT OUTER JOIN cdrdm.dim_time b ON a.time_key = b.time_key;
--WHERE time_key < TBD

INSERT INTO TABLE cdrdm.dim_cell_site_info
SELECT
--FILE_NAME VARCHAR(50),
SITE,
CELL,
ENODEB,
CGI,
CGI_HEX,
SITE_NAME,
ORIGINAL_I,
ANTENNA_TY,
AZIMUTH,
BEAMWIDTH,
X,
Y,
LONGITUDE,
LATITUDE,
ADDRESS,
CITY,
PROVINCE,
ARFCNDL,
BCCHNO,
LOCATIONCO,
BSIC,
PRIMARYSCR,
--START_DATE STRING,
--END_DATE STRING,
--INSERT_TIMESTAMP STRING,
--UPDATE_TIMESTAMP STRING,          
--WORKFLOW_RUN_ID DECIMAL(11,0),
if (substr(FILE_NAME,26,1) = '_', substr(FILE_NAME,23,3), substr(FILE_NAME,23,4)) as FILE_TYPE,
INSERT_TIMESTAMP,
if (substr(FILE_NAME,26,1) = '_', concat(substr(FILE_NAME,27,4),'-',substr(FILE_NAME,31,2),'-',substr(FILE_NAME,33,2)), concat(substr(FILE_NAME,28,4),'-',substr(FILE_NAME,32,2),'-',substr(FILE_NAME,34,2))) as file_date
FROM ext_cdrdm.dim_cell_site_info;