set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT INTO IPTV.MONTHLY_ACTIVE_TEST_ACCOUNTS partition(DT_MONTHLY)
SELECT
	Count(DISTINCT CASE 
			WHEN (IPTV_MAESTRO_STB_AC.EXCLUDE_FLG = 'Y')
				THEN tune_raw_elt_solaris_vw.accountSourceID
			END) accountcount
	,Count(DISTINCT CASE 
			WHEN (IPTV_MAESTRO_STB_AC.EXCLUDE_FLG = 'Y')
				THEN tune_raw_elt_solaris_vw.deviceSourceID
			END) devicecount
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
WHERE
  tune_raw_elt_solaris_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE)		
GROUP BY 	cast(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) 
	,trunc(cast(to_date(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') ;

  
 
