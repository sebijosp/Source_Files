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

SET date_from;
SET date_to;

INSERT OVERWRITE TABLE cdrdm.TIR_VSMSD_LAC_Agg_R27 
SELECT LAC, Description,  
          SUM(Call_Count) Call_Count,
          SUM(Minutes) Minutes, 
          SUM(SMS_Count) SMS_Count,
          SUM(W4G) W4G,
          SUM(W3G) W3G,
          SUM(W2G) W2G,
          SUM(WOther) WOther
FROM cdrdm.V_TIR_VSMSD_DT_LAC_Agg_R27
WHERE UDate BETWEEN 
  '${hiveconf:date_from}'
AND
  '${hiveconf:date_to}'
GROUP BY LAC, Description;
