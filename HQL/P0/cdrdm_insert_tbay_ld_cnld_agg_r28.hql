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


INSERT OVERWRITE TABLE cdrdm.tbay_ld_cnld_agg_r28
SELECT RID, Route_Desc,
          SUM(LD_US_VMIN) LD_US_VMIN,  
          SUM(LD_CANADA_VMIN) LD_CANADA_VMIN, 
          SUM(LD_INTL_VMIN) LD_INTL_VMIN, 
          SUM(LD_INTL_SMS_Cnt) LD_INTL_SMS_Cnt
FROM cdrdm.V_Tbay_DT_LD_CNLD_Agg_R28 
WHERE UDate BETWEEN 
	'${hiveconf:date_from}'
AND
  '${hiveconf:date_to}'
				  
GROUP BY RID, Route_Desc;

