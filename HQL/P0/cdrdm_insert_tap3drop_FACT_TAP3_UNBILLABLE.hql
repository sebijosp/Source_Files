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

INSERT INTO TABLE cdrdm.FACT_TAP3_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
subscriber_no,
case when CHANNEL_SEIZURE_DATE = '' then null else CONCAT(substr(CHANNEL_SEIZURE_DATE,9,2),':',substr(CHANNEL_SEIZURE_DATE,11,2),':',substr(CHANNEL_SEIZURE_DATE,13,2)) end as channel_seizure_time,	
imsi,
duration,
plmn_id,
sender,
recipent,
mps_file_number, 
drop_code,
pdp_addresses,
substr(CONCAT(SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,7)),16) + 1) as int), 1),
SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,6)),16) + 1) as int), 1),
SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,5)),16) + 1) as int), 1),
SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,4)),16) + 1) as int), 1),
SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,3)),16) + 1) as int), 1),
SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,2)),16) + 1) as int), 1),
SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,1)),16) + 1) as int), 1),
SUBSTR('0123456789ABCDEF', cast((PMOD((cell_id / pow(16,0)),16) + 1) as int), 1)), 5) as cell_id,
downlink_volume, 
uplink_volume,
file_sequence_number,
charging_id,
trim(access_point_name),
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
case when CHANNEL_SEIZURE_DATE = '' then null else CONCAT(substr(CHANNEL_SEIZURE_DATE,1,4),'-',substr(CHANNEL_SEIZURE_DATE,5,2),'-',substr(CHANNEL_SEIZURE_DATE,7,2)) end as channel_seizure_date
FROM cdrdm.tap3drop a ${hiveconf:WHERE_clause} AND cell_id <> 'ZZZZZZZZ';