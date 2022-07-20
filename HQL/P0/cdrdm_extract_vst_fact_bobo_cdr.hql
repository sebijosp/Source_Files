SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=mr;
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
set hive.exec.compress.output=false;


SET date_from;
SET date_to;
SET file_directory;

insert overwrite local directory '${hiveconf:file_directory}' row format delimited fields terminated by '|'
SELECT	
fact_bobo_cdr.transaction_id, 
fact_bobo_cdr.transaction_date,
fact_bobo_cdr.transaction_time, 
fact_bobo_cdr.charged_party_spid,
fact_bobo_cdr.transaction_type, 
fact_bobo_cdr.partner_id, 
fact_bobo_cdr.partner_name,
COALESCE(fact_bobo_cdr.provider_id,'') as  provider_id,
fact_bobo_cdr.service_description,
fact_bobo_cdr.total_amount 
FROM	
cdrdm.fact_bobo_cdr fact_bobo_cdr
WHERE	
transaction_date BETWEEN 
  '${hiveconf:date_from}'
AND
  '${hiveconf:date_to}'
group by 
fact_bobo_cdr.transaction_id, 
fact_bobo_cdr.transaction_date,
fact_bobo_cdr.transaction_time, 
fact_bobo_cdr.charged_party_spid,
fact_bobo_cdr.transaction_type, 
fact_bobo_cdr.partner_id, 
fact_bobo_cdr.partner_name,
fact_bobo_cdr.provider_id, 
fact_bobo_cdr.service_description,
fact_bobo_cdr.total_amount;

