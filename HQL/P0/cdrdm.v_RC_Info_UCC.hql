--[Version History]
--0.1 - danchang - 3/17/2016 - updated logic based on reference SQL provided in "Semantic Layer (parts of SQL) v1.txt"
--
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

CREATE VIEW IF NOT EXISTS cdrdm.v_RC_Info_UCC_5 AS
SELECT
a.*,
b.bill_conf_status AS bill_conf_status,
b.cycle_code AS cycle_code,
b.cycle_run_year AS cycle_run_year,
b.cycle_run_month AS cycle_run_month
FROM ela_v21.charge a
LEFT OUTER JOIN 
(SELECT bill_conf_status, cycle_code, cycle_run_year, cycle_run_month, ban, bill_seq_no 
  FROM ela_v21.bill) b 
  ON a.ban = b.ban 
  AND a.actv_bill_seq_no = b.bill_seq_no
LEFT OUTER JOIN 
(SELECT tab_type, EFFECTIVE_DATE, EXPIRATION_DATE, ban, subscriber_no
  FROM cdrdm.dim_ucc_sub_info
  WHERE tab_type IN ('FTR_HIST','FTR')
  --WHERE UPPER(TRIM(tab_type)) IN ('FTR_HIST','FTR')
  ) c 
  ON a.ban = c.ban 
  AND a.subscriber_no = c.subscriber_no
WHERE a.CHG_CREATION_DATE BETWEEN c.EFFECTIVE_DATE AND COALESCE(c.EXPIRATION_DATE,'9999-12-31:00:00:00');