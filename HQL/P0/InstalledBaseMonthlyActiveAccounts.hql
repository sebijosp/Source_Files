set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT OVERWRITE TABLE IPTV.MONTHLY_ACTIVE_ACCOUNTS partition(DT_MONTHLY)
SELECT
 Count(distinct ip_playback_fct.device.accountSourceID) accountcount
 ,Count(distinct ip_playback_fct.device.deviceSourceID) devicecount
 ,MAX(cast(date_format(ip_playback_fct.EVENT_DATE,'yy-MM') AS STRING)) extractmonth
 ,trunc(ip_playback_fct.event_date, 'MM') dt_monthly
FROM iptv.ip_playback_fct
INNER JOIN (SELECT DISTINCT prim_resource_val FROM iptv.IPTV_MAESTRO_STB_AC WHERE INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC
ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL = ip_playback_fct.device.accountsourceid
WHERE
  SUBSTR(from_unixtime(CAST(ip_playback_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) >= 18
  AND SUBSTR(from_unixtime(CAST(ip_playback_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) <= 23
  --AND ip_playback_fct.deliveryMedium IN ('IP')
  --AND trunc(ip_playback_fct.event_date, 'MM') >= '2019-01-01'
  AND trunc(ip_playback_fct.event_date, 'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE), 'MM')
  AND  ip_playback_fct.device.devicetype like 'STB%'
GROUP BY
  trunc(ip_playback_fct.event_date, 'MM');

