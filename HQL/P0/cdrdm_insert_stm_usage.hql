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
set hive.optimize.ppd=true;

INSERT OVERWRITE TABLE cdrdm.STM_USAGE PARTITION (usage_year_month)
SELECT
ALL_VSMS.DT AS USAGE_DT,
ALL_VSMS.NW_CARRIER, 
ALL_VSMS.SID AS SID,
CAST(SUM(ALL_VSMS.TOT_SMS) AS DECIMAL(32,0)) AS TOT_SMS,
CAST(SUM(ALL_VSMS.TOT_VOICE_MIN) AS DECIMAL(32,0)) AS TOT_VOICE_MIN,
CAST(SUM(CAST(ALL_VSMS.DATA_BYTES AS DECIMAL(32,0))) AS DECIMAL(32,0)) AS TOT_DATA_BYTES,
COUNT(DISTINCT ALL_VSMS.IMSI) AS UNQ_SUBS,
DATE_FORMAT(ALL_VSMS.DT,'YYYYMM')  AS usage_year_month
FROM (
select DT,NW_CARRIER,SID,TOT_SMS,TOT_VOICE_MIN,DATA_BYTES,IMSI from cdrdm.stm_usage_step1
union all
select DT,NW_CARRIER,SID,TOT_SMS,TOT_VOICE_MIN,DATA_BYTES,IMSI from cdrdm.stm_usage_step2
union all
select DT,NW_CARRIER,SID,TOT_SMS,TOT_VOICE_MIN,DATA_BYTES,IMSI from cdrdm.stm_usage_step3
union all
select DT,NW_CARRIER,SID,TOT_SMS,TOT_VOICE_MIN,DATA_BYTES,IMSI from cdrdm.stm_usage_step4) ALL_VSMS
GROUP BY ALL_VSMS.DT,ALL_VSMS.NW_CARRIER,ALL_VSMS.SID;

DROP TABLE IF EXISTS cdrdm.STM_USAGE_EXPORT;

create table cdrdm.stm_usage_export
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
stored as textfile
location '/user/hdpcdr/file_export/export/stm_usage/' as
select "USAGE_DT","NW_CARRIER","SID","UNQ_SUBS","TOT_VOICE_MIN","TOT_DATA_BYTES","TOT_SMS" 
union all
select usage_dt,nw_carrier,sid,CAST(unq_subs AS STRING),CAST(tot_voice_min AS STRING),CAST(tot_data_bytes AS STRING),CAST(tot_sms AS STRING) from cdrdm.stm_usage where usage_year_month = concat(substr('${hiveconf:date_from}',1,4), substr('${hiveconf:date_from}',6,2))
ORDER BY "USAGE_DT";
