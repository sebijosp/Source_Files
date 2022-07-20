set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT OVERWRITE TABLE IPTV.MONTHLY_ACTIVE_TEST_ACCOUNTS partition(DT_MONTHLY)
SELECT
  Count(DISTINCT tune_fct.device.accountSourceID) accountcount
  , Count(DISTINCT tune_fct.device.deviceSourceID) devicecount
  , max(cast(from_unixtime(CAST(tune_fct.header.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING)) extractmonth
  , trunc(cast(to_date(from_unixtime(CAST(tune_fct.header.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') dt_monthly  
FROM 
  iptv.tune_fct 
INNER JOIN iptv.IPTV_MAESTRO_STB_AC 
on IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=tune_fct.device.accountsourceid 
WHERE IPTV_MAESTRO_STB_AC.EXCLUDE_FLG = 'Y'
  --AND cast(from_unixtime(CAST(tune_fct.header.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING)  = '2018-09-01'
  AND trunc(cast(to_date(from_unixtime(CAST(tune_fct.header.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE), 'MM')
GROUP BY
  cast(from_unixtime(CAST(tune_fct.header.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) 
  ,trunc(cast(to_date(from_unixtime(CAST(tune_fct.header.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM');
