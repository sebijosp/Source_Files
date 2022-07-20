set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE IPTV.DAILY_ACTIVE_ACCOUNTS PARTITION (DT_MONTHLY)
SELECT  
  tune_fct.event_date as dt_daily
 ,count(distinct tune_fct.device.accountsourceid) 
 ,count(distinct tune_fct.device.devicesourceid) 
 ,MAX(cast(date_format(tune_fct.event_date,'yy-MM') AS STRING)) extractmonth
 ,MAX(TRUNC(tune_fct.event_date,'MM')) dt_monthly
FROM IPTV.TUNE_FCT
INNER JOIN (SELECT DISTINCT PRIM_RESOURCE_VAL FROM IPTV.IPTV_MAESTRO_STB_AC WHERE INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC
ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL = tune_fct.device.accountsourceid
WHERE 
  SUBSTR(from_unixtime(CAST(header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) >= 18
  AND SUBSTR(from_unixtime(CAST(header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) <= 23
  AND tune_fct.deliveryMedium IN ('IP')
  --AND TRUNC(tune_fct.event_date,'MM') >= TRUNC('2018-05-22','MM')
  AND trunc(tune_fct.event_date, 'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE), 'MM')
  GROUP BY 
  tune_fct.event_date
;
