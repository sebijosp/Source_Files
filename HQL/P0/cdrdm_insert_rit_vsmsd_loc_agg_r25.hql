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

SET date_from;
SET date_to;

INSERT OVERWRITE TABLE cdrdm.RIT_VSMSD_Loc_Agg_R25
SELECT 
Cell_ID_comp, 
Route_Desc,  
SUM(Call_Count) AS Call_Count,
SUM(Sum_Dur_Sec) AS Sum_Dur_Sec, 
SUM(Sum_Dur_Sec)/(NVL(SUM(call_count),0)*1.0000) AS Avg_Dur_Sec,
SUM(SMS_Count) AS SMS_Count, 
SUM(SMS_OG_Cnt) AS SMS_OG_Cnt, 
SUM(SMS_IN_Cnt) AS SMS_IN_Cnt,
SUM(Data_Usage_MB) AS Data_Usage_MB
FROM cdrdm.V_RIT_VSMSD_DT_Loc_Agg_R25
WHERE UDate BETWEEN 
  '${hiveconf:date_from}'
AND
  '${hiveconf:date_to}'
GROUP BY Cell_ID_comp,Route_Desc;