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

INSERT INTO TABLE cdrdm.FACT_BOBO_CDR PARTITION (transaction_date)
SELECT
transaction_id,
CASE WHEN datetime = '' THEN NULL ELSE concat(SUBSTR(datetime,9,2),':',SUBSTR(datetime,11,2),':',SUBSTR(datetime,13,2)) END as transaction_time,
SUBSTR(TRIM(charged_party_msisdn),4) as charged_party_msisdn,
charged_party_imsi,
charged_party_imei,
charged_party_spid,
transaction_type,
call_direction as direction,
orig_transaction_id as original_transaction_id,
partner_id,
partner_name_en,
provider_id,
provider_name_en,
service_desc_en as service_description_en,
billing_support_info,
app_support_info as application_support_info,
CASE WHEN transaction_type =1 THEN charge_amt/100 WHEN transaction_type =2 THEN (charge_amt/100)*-1 END AS total_amount,
charged_msisdn_type,
user_id,
source as source_id,
adj_reason_code as reason_code,
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
CASE WHEN datetime = '' THEN NULL ELSE CONCAT(SUBSTR(TRIM(datetime),1,4),'-',SUBSTR(TRIM(datetime),5,2),'-',SUBSTR(TRIM(datetime),7,2)) END as transaction_date
FROM cdrdm.bobo a ${hiveconf:WHERE_clause};