set hive.execution.engine=mr;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT INTO IPTV.ERRORS_PER_HOUR partition(DT_MONTHLY)
SELECT X.calendar_date dt_daily
	,Count(error_raw_elt_solaris_vw.accountsourceid) Accounts
	,(ip_playback_raw_elt_solaris_vw.sessionDuration) / (1000 * 3600) Hours
	,((ip_playback_raw_elt_solaris_vw.sessionDuration) / (1000 * 3600)) / (Count(error_raw_elt_solaris_vw.accountsourceid)) HoursPerErrorAccounts
	,Count(error_raw_elt_solaris_vw.uuid) Total_Error_Response_Codes
	,(Count(error_raw_elt_solaris_vw.uuid)) / ((ip_playback_raw_elt_solaris_vw.sessionDuration) / (1000 * 3600)) ErrorsPerHour
	,cast(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) extractmonth
	,trunc(cast(to_date(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') dt_monthly
FROM iptv.error_raw_elt_solaris_vw
INNER JOIN iptv.IPTV_MAESTRO_STB_AC ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL = error_raw_elt_solaris_vw.accountsourceid
INNER JOIN (
	SELECT calendar_date
		,military_hour
	FROM (
		SELECT *
		FROM app_shared_dim.lu_date
		) A
	CROSS JOIN (
		SELECT DISTINCT military_hour
		FROM app_shared_dim.lu_time_interval
		) B
	) X ON X.calendar_date = cast(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd') AS DATE)
INNER JOIN iptv.ip_playback_raw_elt_solaris_vw ON error_raw_elt_solaris_vw.accountsourceid = ip_playback_raw_elt_solaris_vw.accountsourceid
	AND error_raw_elt_solaris_vw.devicesourceid = ip_playback_raw_elt_solaris_vw.devicesourceid
WHERE military_hour IN (
		18
		,19
		,20
		,21
		,22
		,23
		)
	AND IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y'
AND error_raw_elt_solaris_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE)
GROUP BY X.calendar_date
	,error_raw_elt_solaris_vw.accountsourceid
	,ip_playback_raw_elt_solaris_vw.sessionDuration
	,error_raw_elt_solaris_vw.uuid
	,cast(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) 
	,trunc(cast(to_date(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') ;	
