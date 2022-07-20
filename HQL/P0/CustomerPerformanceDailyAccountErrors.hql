set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT OVERWRITE TABLE IPTV.DAILY_ACCOUNT_ERRORS  PARTITION (dt_monthly)
SELECT 
  error_fct.event_date dt_daily
  , count(distinct error_fct.device.accountsourceid) as unique_error_count
  ,max(date_format(error_fct.event_date,'yy-MM'))   as extractmonth
  ,max(TRUNC(error_fct.event_date,'MM')) dt_monthly
FROM iptv.error_fct
INNER JOIN (SELECT DISTINCT PRIM_RESOURCE_VAL FROM iptv.IPTV_MAESTRO_STB_AC WHERE INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC 
ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL = error_fct.device.accountsourceid
WHERE 
   SUBSTR(from_unixtime(CAST(header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) >= 18
  AND SUBSTR(from_unixtime(CAST(header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) <= 23
  --AND TRUNC(error_fct.event_date,'MM') >= TRUNC('2018-05-01','MM')
  AND TRUNC(error_fct.event_date,'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE),'MM')
GROUP BY
  error_fct.event_date;
