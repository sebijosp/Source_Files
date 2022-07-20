set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE IPTV.DAILY_ERRORS  PARTITION (dt_monthly)
SELECT
  CAST(error_fct.event_date as DATE) as dt_daily
  ,IPTV_STB_ERRORS_CLASSIFICATION.error_code_type
  ,error_fct.errorCode
  ,IPTV_STB_Errors_Classification.Description
  , count(error_fct.errorCode) as error_count
  , max(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT),"yy-MM"))   as extractmonth
  ,max(trunc(cast(to_date(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT), 'yyyy-MM-dd')) AS string), 'MM')) dt_monthly
  FROM 
  iptv.error_fct
INNER JOIN (SELECT distinct prim_resource_val FROM iptv.IPTV_MAESTRO_STB_AC WHERE IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC 
ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=error_fct.device.accountsourceid
LEFT OUTER JOIN IPTV.IPTV_STB_Errors_Classification 
ON IPTV_STB_ERRORS_CLASSIFICATION.Error_Code = error_fct.errorCode
WHERE 
  --TRUNC(error_fct.event_date,'MM') =TRUNC('2018-05-01','MM')
  TRUNC(error_fct.event_date,'MM')>=TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE),'MM')
GROUP BY
  error_fct.event_date
  ,IPTV_STB_ERRORS_CLASSIFICATION.error_code_type
  ,error_fct.errorCode
  ,IPTV_STB_Errors_Classification.Description
;
