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

INSERT INTO TABLE cdrdm.FACT_GLENAYRE_CDR PARTITION (file_date)
SELECT
substr(data,3,16) as subscriber_no,
substr(data,3,16) as subscriber_no_char,
substr(data,1,2) as record_type,
substr(data,3,16) as access_number,
concat(substr(substr(data,19,6), 1, 2),':',substr(substr(data,19,6), 3, 2),':',substr(substr(data,19,6), 5, 2)) as collection_time,
substr(data,25,2) as return_code,
substr(data,27,2) as service_type,
substr(data,29,2) as encoder_type,
trim(substr(data,31,14)) as capcode,
substr(data,45,5) as coverage_region,
cast(substr(data,50,5) as int) as call_count,
cast(substr(data,55,8) as int) as character_count,
cast(substr(data,63,10) as int) as vm_storage_seconds,
cast(substr(data,73,5) as int) as meetme_connect_seconds,
substr(data,78,1) as customer_enabled,
substr(data,79,1) as customer_absent,
trim(substr(data,3,6)) as switch_id,
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
file_date
FROM cdrdm.glenayre a ${hiveconf:WHERE_clause} AND substr(data,1,7) <> CONCAT('99',UPPER(switch_name));