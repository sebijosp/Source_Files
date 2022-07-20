--[Version History]
--0.1 - danchang - 3/17/2016 - updated logic based on reference SQL provided in "Semantic Layer (parts of SQL) v1.txt"
--0.2 - danchang - 4/25/2016 - updated for PROD - removed test table references
--

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
SET hive.tez.log.level=DEBUG;

CREATE VIEW IF NOT EXISTS cdrdm.v_USG_UCC AS
SELECT
a.*,
b.bill_conf_status,
IF(MESSAGE_SWITCH_ID LIKE 'UCC%' OR TR_Sub_UCC_IND IS NOT NULL, 'UCC', NULL) AS UCC_USG,
IF(TRIM(toll_type) = 2 AND TRIM(US_Stream_type) = 'V', rounded_units, 0) AS Local_MIN,
IF(TRIM(toll_type) = 2 AND TRIM(US_Stream_type) = 'V', 1, 0) AS Local_Cnt,
IF(TRIM(toll_type) = 3 AND TRIM(US_Stream_type) = 'V', rounded_units, 0) AS LD_US_MIN,
IF(TRIM(toll_type) = 3 AND TRIM(US_Stream_type) = 'V', 1, 0) AS LD_US_Cnt,
IF(TRIM(toll_type) = 4 AND TRIM(US_Stream_type) = 'V', rounded_units, 0) AS LD_CND_MIN,
IF(TRIM(toll_type) = 4 AND TRIM(US_Stream_type) = 'V', 1, 0) AS LD_CND_Cnt,
IF(TRIM(toll_type) = 5 AND TRIM(US_Stream_type) = 'V', rounded_units, 0) AS LD_INTL_MIN,
IF(TRIM(toll_type) = 5 AND TRIM(US_Stream_type) = 'V', 1, 0) AS LD_INTL_Cnt,
IF(TRIM(toll_type) = 2 AND TRIM(US_Stream_type) = 'M', 1, 0) AS DOM_SMS_Cnt,
IF(TRIM(toll_type) IN (3,4,5) AND TRIM(US_Stream_type) = 'M', 1, 0) AS INT_SMS_Cnt,
IF(TRIM(record_type_ind) = 'L' AND TRIM(US_Stream_type) = 'D', rounded_units, 0) AS LTE_DATA_KB,
IF(TRIM(record_type_ind) <> 'L' AND TRIM(US_Stream_type) = 'D', rounded_units, 0) AS Non_LTE_DATA_KB
FROM ods_usage.usage a LEFT OUTER JOIN ela_v21.bill b ON a.ban = b.ban AND a.bill_seq_no = b.bill_seq_no;