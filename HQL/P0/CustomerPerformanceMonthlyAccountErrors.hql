set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT OVERWRITE TABLE IPTV.MONTHLY_ACCOUNT_ERRORS partition (DT_MONTHLY)
SELECT 
  count(distinct error_fct.device.accountsourceid) as unique_error_count
  , max(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT),"yy-MM"))   as extractmonth
  , trunc(cast(to_date(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT), 'yyyy-MM-dd')) AS string), 'MM') dt_monthly
FROM 
IPTV.ERROR_FCT 
INNER JOIN (SELECT DISTINCT PRIM_RESOURCE_VAL FROM iptv.IPTV_MAESTRO_STB_AC WHERE INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC
ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=error_fct.device.accountsourceid 
WHERE 
SUBSTR(from_unixtime(CAST(header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) >= 18
AND SUBSTR(from_unixtime(CAST(header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) <= 23
--AND  trunc(cast(to_date(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT), 'yyyy-MM-dd')) AS string), 'MM') >= '2018-09-01'
AND  trunc(cast(to_date(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT), 'yyyy-MM-dd')) AS string), 'MM') >= trunc(cast('${hiveconf:RUN_DATE}' AS DATE),'MM')
GROUP BY
trunc(cast(to_date(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT), 'yyyy-MM-dd')) AS string), 'MM')
;
