set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT INTO IPTV.DAILY_ERRORS  PARTITION (dt_monthly)
SELECT cast(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'YYYY-MM-dd') AS DATE) dt_daily
	,IPTV_STB_ERRORS_CLASSIFICATION.error_code_type
	,error_raw_elt_solaris_vw.errorCode
	,IPTV_STB_Errors_Classification.Description
	,Count(CASE 
			WHEN military_hour >= 18
				AND military_hour <= 23
				THEN error_raw_elt_solaris_vw.accountsourceid
			END) errorcount
	,cast(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) extractmonth
	,trunc(cast(to_date(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') dt_monthly
FROM IPTV.IPTV_STB_Errors_Classification
INNER JOIN iptv.error_raw_elt_solaris_vw ON IPTV_STB_ERRORS_CLASSIFICATION.Error_Code = error_raw_elt_solaris_vw.errorCode
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
WHERE error_raw_elt_solaris_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE)
GROUP BY cast(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'YYYY-MM-dd') AS DATE)
	,IPTV_STB_ERRORS_CLASSIFICATION.error_code_type
	,error_raw_elt_solaris_vw.errorCode
	,IPTV_STB_Errors_Classification.Description
	,cast(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING)
	,trunc(cast(to_date(from_unixtime(CAST(error_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM');

