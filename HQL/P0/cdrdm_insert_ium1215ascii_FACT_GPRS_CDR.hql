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

INSERT INTO TABLE cdrdm.FACT_GPRS_CDR PARTITION (record_opening_date)
SELECT
if((served_msisdn is not NULL) and length(trim(served_msisdn)) > 0, substr(trim(served_msisdn),2,10), NULL) as subscriber_no,
if((served_msisdn is not NULL) and length(trim(served_msisdn)) > 0, substr(trim(served_msisdn),2,10), NULL) as subscriber_no_char,
if((record_sequence_number is not NULL) and length(trim(record_sequence_number)) > 0, record_sequence_number, 0) as record_sequence_number,
gprs_choice_mask_archive,
served_imsi,
if((record_opening_time is not NULL) and length(trim(record_opening_time)) > 0, concat(SUBSTR(record_opening_time,9,2),':',SUBSTR(record_opening_time,11,2),':',SUBSTR(record_opening_time,13,2)), NULL) as record_opening_time,
served_msisdn,
if((data_volume_uplink_archive is not NULL) and length(trim(data_volume_uplink_archive)) > 0, data_volume_uplink_archive, NULL),
if((data_volume_downlink_archive is not NULL) and length(trim(data_volume_downlink_archive)) > 0, data_volume_downlink_archive, NULL),
routing_area,
location_area_code,
trim(access_point_name),
SUBSTR(served_msisdn,4,6) as reporting_centre_id, --cast(int)
if((destination_url is not NULL) and length(trim(destination_url)) > 0, trim(destination_url), NULL),
if((spid is not NULL) and length(trim(spid)) > 0, trim(spid), NULL),
if((service_class_group is not NULL) and length(trim(service_class_group)) > 0, trim(service_class_group), NULL),
if((content_delivered is not NULL) and length(trim(content_delivered)) > 0, trim(content_delivered), NULL),
if((event_protocol_type is not NULL) and length(trim(event_protocol_type)) > 0, trim(event_protocol_type), NULL),
if((wireless_generation is not NULL) and length(trim(wireless_generation)) > 0, trim(wireless_generation), NULL),
if((event_count is not NULL) and length(trim(event_count)) > 0,if(trim(event_count)='ZZZZ','0000',trim(event_count)),NULL),
if((domain_1 is not NULL) and length(trim(domain_1)) > 0, trim(domain_1), NULL),
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_identifier,
duration,
charging_id,
cell_identifier,
customer_type,
CONCAT(trim(plmn_identifier),trim(location_area_code),trim(cell_identifier)) as monum,
tracking_area_code,
eutran_cellid,
if((record_opening_time is not NULL) and length(trim(record_opening_time)) > 0, SUBSTR(record_opening_time,15,5), NULL) as record_opening_date_offset,
file_name,
record_start,
record_end,
record_type,
'ium1215ascii', --family_name
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()),
if((record_opening_time is not NULL) and length(trim(record_opening_time)) > 0, concat(SUBSTR(record_opening_time,1,4),'-',SUBSTR(record_opening_time,5,2),'-',SUBSTR(record_opening_time,7,2)), NULL) as record_opening_date
FROM cdrdm.ium1215ascii a ${hiveconf:WHERE_clause};