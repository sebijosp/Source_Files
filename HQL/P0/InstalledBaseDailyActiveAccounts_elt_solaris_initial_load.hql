set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT INTO IPTV.DAILY_ACTIVE_ACCOUNTS PARTITION(DT_MONTHLY)
SELECT DISTINCT cast(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'YYYY-MM-dd') AS DATE) dt_daily
	,tune_raw_elt_solaris_vw.accountsourceid
	,tune_raw_elt_solaris_vw.devicesourceid
	,cast(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) extractmonth
	,trunc(cast(to_date(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') dt_monthly
FROM iptv.tune_raw_elt_solaris_vw
INNER JOIN iptv.IPTV_MAESTRO_STB_AC ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL = tune_raw_elt_solaris_vw.accountsourceid
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
	) X ON X.calendar_date = cast(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd') AS DATE)
WHERE military_hour IN (
		18
		,19
		,20
		,21
		,22
		,23
		)
	AND tune_raw_elt_solaris_vw.deliveryMedium IN ('IP')
	AND IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y'
	 AND tune_raw_elt_solaris_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE)	;
