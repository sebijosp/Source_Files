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

INSERT INTO TABLE cdrdm.FACT_SGW_DROP_CDR PARTITION (record_opening_date)
SELECT
trim(error_codes),
cast(trim(gprs_choice_mask) as int),
trim(served_imsi),
CONCAT(SUBSTR(record_opening_time,1,4),'-',SUBSTR(record_opening_time,5,2),'-',SUBSTR(record_opening_time,7,2),' ',SUBSTR(record_opening_time,9,2),':',SUBSTR(record_opening_time,11,2),':',SUBSTR(record_opening_time,13,2)),
trim(served_msisdn),
cast(trim(data_volume_gprs_uplink) as int),
cast(trim(data_volume_gprs_downlink) as int),
trim(tracking_area_code),
trim(access_point_name),
trim(enode),
trim(charging_id),
cast(trim(duration) as int),
cast(trim(cause_for_term) as int),
trim(pdp_ip_address),
trim(pgw_ip_address),
trim(sgw_ip_address),
switch_id,
served_imei,
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
CONCAT(SUBSTR(record_opening_time,1,4),'-',SUBSTR(record_opening_time,5,2),'-',SUBSTR(record_opening_time,7,2)) as record_opening_date
FROM cdrdm.sgw1215drop a ${hiveconf:WHERE_clause};