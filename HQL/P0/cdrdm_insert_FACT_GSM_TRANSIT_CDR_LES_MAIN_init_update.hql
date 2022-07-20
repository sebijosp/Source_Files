use cdrdm;
set hive.enforce.bucketing = true;
SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
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
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
--SET hive.exec.orc.default.buffer.size=32768;
set hive.tez.auto.reducer.parallelism=true;
--set hive.tez.min.partition.factor=0.25;
--set hive.tez.max.partition.factor=2.0;
--SET mapred.max.split.size=1000000;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.exec.parallel=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.dbclass=fs;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true; 
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;


INSERT into CDRDM.LES_MAIN PARTITION (call_TIMESTAMP_DATE)
SELECT LES_DATA.call_type,
LES_DATA.calling_number,
LES_DATA.called_number,
LES_DATA.original_called_number,
LES_DATA.redirecting_number,
LES_DATA.chargeable_duration,
LES_DATA.First_Cell_complete_Orig AS First_Cell_complete,
LES_DATA.Last_Cell_complete_Orig AS Last_Cell_complete,
LES_DATA.record_seq_number,
LES_DATA.switch_name,
LES_DATA.first_cell_id_extension,
LES_DATA.subscriptionType, 
LES_DATA.SRVCCIndicator, 
NULL AS IsTbaytel,
LES_DATA.CALL_DATE,
current_timestamp,
LES_DATA.called_serial_number,
LES_DATA.calling_serial_number,
LES_DATA.imsi,
LES_DATA.subject_number,
LES_DATA.call_timestamp_date
FROM CDRDM.les_main_fact_gsm_trnst_cdr_step2 LES_DATA;

truncate table cdrdm.fact_gsm_trnst_cdr_stg;
truncate table cdrdm.les_main_fact_gsm_trnst_cdr_step2;
truncate table cdrdm.les_main_fact_gsm_trnst_cdr_step1;
