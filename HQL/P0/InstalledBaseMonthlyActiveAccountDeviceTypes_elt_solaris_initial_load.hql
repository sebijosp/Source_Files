set hive.execution.engine=mr;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;


INSERT INTO IPTV.MONTHLY_ACTIVE_ACC_DEVICE_TYPE PARTITION (DT_MONTHLY)
SELECT 
	ip_playback_raw_elt_solaris_vw.appName DeviceType
	,count(ip_playback_raw_elt_solaris_vw.accountsourceid) AccountCount
	,cast(from_unixtime(CAST(ip_playback_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) extractmonth
	,trunc(cast(to_date(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') dt_monthly
FROM
iptv.ip_playback_raw_elt_solaris_vw inner join  iptv.IPTV_MAESTRO_STB_AC
on IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=ip_playback_raw_elt_solaris_vw.accountsourceid 
inner join iptv.tune_raw_elt_solaris_vw on tune_raw_elt_solaris_vw.accountsourceid=ip_playback_raw_elt_solaris_vw.accountsourceid
and tune_raw_elt_solaris_vw.devicesourceid=ip_playback_raw_elt_solaris_vw.devicesourceid
WHERE IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y'
AND ip_playback_raw_elt_solaris_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE)
AND tune_raw_elt_solaris_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE)
GROUP BY
	ip_playback_raw_elt_solaris_vw.appName 
	,cast(from_unixtime(CAST(ip_playback_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) 
	,trunc(cast(to_date(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') ;
