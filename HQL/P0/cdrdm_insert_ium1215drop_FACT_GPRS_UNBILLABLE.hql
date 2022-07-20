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

INSERT INTO TABLE cdrdm.FACT_GPRS_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
if((subscriber_no is not NULL) and length(trim(subscriber_no)) > 0, subscriber_no, NULL) as subscriber_no,
if((subscriber_no is not NULL) and length(trim(subscriber_no)) > 0, subscriber_no, NULL) as subscriber_no_char,
if((channel_seizure_date is not NULL) and length(trim(channel_seizure_date)) > 0, concat(SUBSTR(channel_seizure_date,9,2),':',SUBSTR(channel_seizure_date,11,2),':',SUBSTR(channel_seizure_date,13,2)), NULL) as channel_seizure_time,
call_action_code,
if((unit_esn is not NULL) and length(trim(unit_esn)) > 0, trim(unit_esn), NULL),
if((ring_time is not NULL) and length(trim(ring_time)) > 0, cast(ring_time as int),NULL),
if((at_call_dur_sec is not NULL) and length(trim(at_call_dur_sec)) > 0, cast((at_call_dur_sec/100) as int), NULL),
call_term_code,
call_to_tn,
serv_sid,
home_sid,
mps_file_number,
toll_type,
market_code,
message_type,
call_direction_code,
if((characteristics_1 is not NULL) and length(trim(characteristics_1)) > 0, trim(characteristics_1),NULL),
if((error_codes is not NULL) and length(trim(error_codes)) > 0, substr(trim(error_codes),6),NULL),
struct_code,
call_type,
units_uom,
gprs_choice_mask,
served_imsi,
if((record_opening_time is not NULL) and length(trim(record_opening_time)) > 0, concat(SUBSTR(record_opening_time,9,2),':',SUBSTR(record_opening_time,11,2),':',SUBSTR(record_opening_time,13,2)), NULL) as record_opening_time,
trim(served_msisdn),
if((data_volume_gprs_uplink is not NULL) and length(trim(data_volume_gprs_uplink)) > 0, trim(data_volume_gprs_uplink), NULL),
if((data_volume_gprs_downlink is not NULL) and length(trim(data_volume_gprs_downlink)) > 0, trim(data_volume_gprs_downlink), NULL),
routing_area,
location_area_code,
trim(access_point_name),
message_switch_id,
orig_cell_trunk_id,
term_cell_trunk_id,
NULL, --reporting_centre_id
substr(trim(error_codes),1,4) as unbillable_reason_codes,
substr(trim(error_codes),1,1) as format_code, --format_code
if((destination_url is not NULL) and length(trim(destination_url)) > 0, destination_url, NULL),
trim(spid),
if((service_class_group is not NULL) and length(trim(service_class_group)) > 0, service_class_group, NULL),
trim(content_delivered),
trim(event_protocol_type),
trim(wireless_generation),
if((event_count is not NULL) and length(trim(event_count)) > 0, event_count, NULL),
if((domain_1 is not NULL) and length(trim(domain_1)) > 0, domain_1, NULL),
plmn_id,
cell_id,
sgsn_ip,
concat(plmn_id,location_area_code,cell_id) as monum,
NULL, --charging_id
NULL, --duration
NULL, --node_id
tracking_area_code,
eutran_cellid,
file_name,
record_start,
record_end,
record_type,
'ium1215drop', --family_name
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()),
if((channel_seizure_date is not NULL) and length(trim(channel_seizure_date)) > 0, concat(substr(channel_seizure_date,1,4),'-',substr(channel_seizure_date,5,2),'-',substr(channel_seizure_date,7,2)),NULL) as channel_seizure_date
FROM cdrdm.ium1215drop a ${hiveconf:WHERE_clause};