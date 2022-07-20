set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

truncate table IPTV.DAILY_ACTIVE_ACCOUNTS;
INSERT INTO IPTV.DAILY_ACTIVE_ACCOUNTS (DT_MONTHLY,DT_DAILY,ACCOUNTCOUNT,DEVICECOUNT)
SELECT DISTINCT trunc(cast(to_date(X.calendar_date) AS string), 'MM')
	,X.calendar_date
	,tune_raw_vw.accountsourceid
	,tune_raw_vw.devicesourceid
FROM iptv.tune_raw_vw
INNER JOIN iptv.IPTV_MAESTRO_STB_AC ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL = tune_raw_vw.accountsourceid
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
	) X ON X.calendar_date = cast(from_unixtime(CAST(tune_raw_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd') AS DATE)
WHERE military_hour IN (
		18
		,19
		,20
		,21
		,22
		,23
		)
	AND tune_raw_vw.deliveryMedium IN ('IP')
	AND IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y'
	AND tune_raw_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE);	
