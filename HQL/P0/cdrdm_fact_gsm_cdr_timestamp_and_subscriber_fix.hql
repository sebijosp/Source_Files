--SET hive.variable.substitute=true;
SET hive.auto.convert.join=false;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=mr;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
--SET hive.cbo.enable=true;
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
SET hive.optimize.index.filter=false;

USE cdrdm;

DROP TABLE Z_FACT_GSM_CDR_Temp;

CREATE TABLE Z_FACT_GSM_CDR_Temp(
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

INSERT INTO TABLE Z_FACT_GSM_CDR_Temp PARTITION (call_timestamp_date) 
SELECT 
subscriber_no,
record_sequence_number,
call_type,
imsi,
imei,
exchange_identity,
switch_identity,
substr(call_timestamp,1,19) as call_timestamp,
chargeable_duration,
calling_party_number,
cleansed_calling_number,
called_party_number,
cleansed_called_number,
original_called_number,
cleansed_original_number,
reg_seizure_charging_start,
mobile_station_roaming_number,
redirecting_number,
cleansed_redirecting_number,
fault_code,
eos_info,
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
charged_party,
charged_party_number,
cleansed_charged_number,
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
partial_output_num,
rco,
ocn,
multimediacall,
teleservicecode,
tariffclass,
first_cell_id_extension,
subscriptiontype,
srvccindicator,
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
call_timestamp_date
FROM FACT_GSM_CDR where file_name IN ('FACT_GSM_CDR', 'FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST', 'FACT_GSM_OUT_SMS_HIST', 'FACT_GSM_IN_SMS_HIST');

INSERT INTO TABLE Z_FACT_GSM_CDR_Temp PARTITION (call_timestamp_date) 
SELECT
CASE WHEN call_type = 3 AND subscriber_no = 0 THEN cleansed_called_number ELSE subscriber_no END as subscriber_no,
record_sequence_number,
call_type,
imsi,
imei,
exchange_identity,
switch_identity,
call_timestamp,
chargeable_duration,
calling_party_number,
cleansed_calling_number,
called_party_number,
cleansed_called_number,
original_called_number,
cleansed_original_number,
reg_seizure_charging_start,
mobile_station_roaming_number,
redirecting_number,
cleansed_redirecting_number,
fault_code,
eos_info,
incoming_route_id,
outgoing_route_id,
first_cell_id,
last_cell_id,
charged_party,
charged_party_number,
cleansed_charged_number,
internal_cause_and_loc,
traffic_activity_code,
disconnecting_party,
partial_output_num,
rco,
ocn,
multimediacall,
teleservicecode,
tariffclass,
first_cell_id_extension,
subscriptiontype,
srvccindicator,
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
call_timestamp_date
FROM FACT_GSM_CDR where file_name NOT IN ('FACT_GSM_CDR', 'FACT_GSM_IN_VOICE_HIST', 'FACT_GSM_OUT_VOICE_HIST', 'FACT_GSM_CF_HIST', 'FACT_GSM_CFR_HIST', 'FACT_GSM_OUT_SMS_HIST', 'FACT_GSM_IN_SMS_HIST');