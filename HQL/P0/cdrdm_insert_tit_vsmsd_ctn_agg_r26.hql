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

INSERT OVERWRITE TABLE cdrdm.TIT_VSMSD_CTN_Agg_R26
SELECT Subscriber_no, SUM(minutes) Minutes, 
       SUM(W4G)/1048576.00000000 W4G, SUM(W3G)/1048576.00000000 W3G,  
       SUM(W2G)/1048576.00000000 W2G, SUM(WOther)/1048576.00000000 WOther, 
       SUM(CASE WHEN UType = 'SMS' THEN cnt ELSE 0 END) SMS_cnt
FROM 
(
 SELECT UDate, Subscriber_no, UType, COALESCE(minutes,0) minutes,cnt, 
            CAST(0 AS BIGINT) AS W4G, CAST(0 AS BIGINT)  AS W3G, 
            CAST(0 AS BIGINT)  AS W2G, CAST(0 AS BIGINT)  AS WOther   
  FROM cdrdm.V_TIT_sum_SMSVoice_CTN
  
  UNION ALL
  
  SELECT UDate, Subscriber_no, UType, 0 AS minutes,cnt, 
            W4G, W3G,W2G, WOther 
  FROM cdrdm.V_TIT_sum_UpDownBytes_CTN
) RP26CTN 
WHERE UDate BETWEEN 
  '${hiveconf:date_from}'
AND
  '${hiveconf:date_to}'
GROUP BY Subscriber_no;
