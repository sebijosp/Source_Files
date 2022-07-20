set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT OVERWRITE TABLE IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE PARTITION (DT_MONTHLY)
SELECT
  CASE WHEN ip_playback_fct.device.devicetype like 'STB%' THEN 'SetTopBox' ELSE 'SecondScreen' END DeviceType
  ,count(distinct ip_playback_fct.device.accountsourceid) AccountCount
  ,MAX(cast(date_format(ip_playback_fct.EVENT_DATE,'yy-MM') AS STRING)) extractmonth
  ,trunc(ip_playback_fct.EVENT_DATE, 'MM') dt_monthly
FROM
  iptv.ip_playback_fct
  INNER JOIN (SELECT DISTINCT PRIM_RESOURCE_VAL FROM iptv.IPTV_MAESTRO_STB_AC WHERE INCLUDE_FLG = 'Y') IPTV_MAESTRO_STB_AC
  ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=ip_playback_fct.device.accountsourceid
  WHERE
  SUBSTR(from_unixtime(CAST(ip_playback_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) >= 18
  AND SUBSTR(from_unixtime(CAST(ip_playback_fct.header.`TIMESTAMP`/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss"),12,2) <= 23
  --AND trunc(ip_playback_fct.event_date, 'MM') = TRUNC('2019-05-01','MM')
  AND trunc(ip_playback_fct.event_date, 'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE), 'MM')
  GROUP BY
  CASE WHEN ip_playback_fct.device.devicetype like 'STB%' THEN 'SetTopBox' ELSE 'SecondScreen' END
  ,trunc(ip_playback_fct.EVENT_DATE, 'MM')
;

