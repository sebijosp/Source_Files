set hive.execution.engine=mr;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT OVERWRITE TABLE IPTV.ERRORS_PER_HOUR partition(DT_MONTHLY)
SELECT 
    error_fct.dt_daily
  , ip_playback_fct_ac.Accounts
  , cast(ip_playback_fct.Hours as decimal(10,2)) Hours
  , cast((ip_playback_fct.HoursPerErrorAccounts/ip_playback_fct_ac.Accounts) as decimal(6,2)) HoursPerErrorAccounts
  , error_fct.Total_Error_Response_Codes
  , cast(ip_playback_fct.ErrorsPerHour as decimal(6,2)) ErrorsPerHour
  , error_fct.extractmonth
  , error_fct.dt_monthly
FROM
  (SELECT
     CAST(error_fct.event_date as DATE) as dt_daily
      , Count(distinct error_fct.header.uuid) Total_Error_Response_Codes
      , max(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT),"yy-MM"))   as extractmonth
      , max(trunc(cast(to_date(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT), 'yyyy-MM-dd')) AS string), 'MM')) dt_monthly
   FROM 
    iptv.error_fct 
   INNER JOIN  (SELECT distinct prim_resource_val FROM iptv.IPTV_MAESTRO_STB_AC WHERE IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC 
   ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=error_fct.device.accountsourceid 
   WHERE   SUBSTR(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) >= 18
   AND SUBSTR(from_unixtime(CAST(error_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) <= 23 
   AND TRUNC(error_fct.event_date,'MM') >= TRUNC('2018-10-18','MM')
   --AND TRUNC(error_fct.event_date,'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE),'MM')
   GROUP BY
     error_fct.event_date
   ) error_fct
INNER JOIN
  (SELECT
   CAST(ip_playback_fct.event_date as DATE) as dt_daily
   , sum((ip_playback_fct.sessionDuration)/(1000*3600)) Hours
   , (sum(ip_playback_fct.sessionDuration)/(1000*3600)) HoursPerErrorAccounts
   ,(sum(ip_playback_fct.sessionDuration)/(1000*3600))/(Count(ip_playback_fct.device.accountsourceid)) ErrorsPerHour
  FROM iptv.ip_playback_fct 
  inner join (SELECT distinct prim_resource_val FROM iptv.IPTV_MAESTRO_STB_AC WHERE IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC 
  on IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=ip_playback_fct.device.accountsourceid
  WHERE SUBSTR(from_unixtime(CAST(ip_playback_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) >= 18
  AND SUBSTR(from_unixtime(CAST(ip_playback_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) <= 23 
  AND TRUNC(ip_playback_fct.event_date,'MM') >= TRUNC('2018-10-18','MM')
  --AND TRUNC(ip_playback_fct.event_date,'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE),'MM')
  GROUP BY
   ip_playback_fct.event_date
  ) ip_playback_fct
ON error_fct.dt_daily = ip_playback_fct.dt_daily
INNER JOIN
  (SELECT
   CAST(ip_playback_fct.event_date as DATE) as dt_daily
   ,Count(distinct ip_playback_fct.device.accountsourceid) Accounts
  FROM iptv.ip_playback_fct 
  WHERE 
  TRUNC(ip_playback_fct.event_date,'MM') >= TRUNC('2018-10-18','MM')
  --TRUNC(ip_playback_fct.event_date,'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE),'MM')
  GROUP BY
   ip_playback_fct.event_date
   )ip_playback_fct_ac
ON error_fct.dt_daily = ip_playback_fct_ac.dt_daily; 
