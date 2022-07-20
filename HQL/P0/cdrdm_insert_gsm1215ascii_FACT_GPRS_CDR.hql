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
if((served_msisdn is not NULL) and length(trim(served_msisdn)) > 0, substr(trim(served_msisdn),4,10), NULL) as subscriber_no,
SUBSTR(served_msisdn, 4, 10) as subscriber_no_char,
if((record_sequence_no is not NULL) and length(trim(record_sequence_no)) > 0, trim(record_sequence_no), 0) as record_sequence_number,
gprs_choice_mask_archive,
served_imsi,
if((record_opening_time is not NULL) and length(trim(record_opening_time)) > 0, concat(SUBSTR(record_opening_time,9,2),':',SUBSTR(record_opening_time,11,2),':',SUBSTR(record_opening_time,13,2)), NULL) as record_opening_time,
served_msisdn,
if((data_volume_uplink_archive is not NULL) and length(trim(data_volume_uplink_archive)) > 0, data_volume_uplink_archive, NULL),
if((data_volume_downlink_archive is not NULL) and length(trim(data_volume_downlink_archive)) > 0, data_volume_downlink_archive, NULL),
routing_area,
location_area_code,
access_point_name,
SUBSTR(served_msisdn,4,6) as reporting_centre_id,
NULL, --destination_url
NULL, --spid
NULL, --service_class_group
NULL, --content_delivered
NULL, --event_protocol_type
NULL, --wireless_generation
NULL, --event_count
NULL, --domain_1
cdr_type_ind,
NULL, --served_imei
NULL, --sgsn_address
NULL, --served_pdp_address
NULL, --plmn_id
NULL, --duration
NULL, --charging_id
cell_identifier,
NULL, --customer_type
NULL, --monum
NULL, --tracking_area_code
NULL, --eutran_cellid
NULL, --rec_opening_dt_offset
file_name,
record_start,
record_end,
record_type,
'gsm1215ascii', --family_name
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()),
if((record_opening_time is not NULL) and length(trim(record_opening_time)) > 0, concat(SUBSTR(record_opening_time,1,4),'-',SUBSTR(record_opening_time,5,2),'-',SUBSTR(record_opening_time,7,2)), NULL) as record_opening_date
FROM cdrdm.gsm1215ascii a ${hiveconf:WHERE_clause};