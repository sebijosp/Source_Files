set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT INTO IPTV.MONTHLY_TUNE_TYPES partition (DT_MONTHLY)
SELECT (
		CASE 
			WHEN trim(tune_raw_elt_solaris_vw.assetClass) IN (
					'VOD'
					,'IVOD'
					)
				THEN 'VOD'
			WHEN trim(tune_raw_elt_solaris_vw.assetClass) IN ('CDVR')
				THEN 'DVR'
			WHEN trim(tune_raw_elt_solaris_vw.assetClass) IN ('Linear')
				THEN 'Linear'
			END
		) tunetype
	,tune_raw_elt_solaris_vw.tuneStatus
	,tune_raw_elt_solaris_vw.assetClass
	,Sum(CASE 
			WHEN (
					tune_raw_elt_solaris_vw.tuneStatus = 'Success'
					AND tune_raw_elt_solaris_vw.latency > 0
					)
				THEN tune_raw_elt_solaris_vw.latency
			END) latency
	,Sum(CASE 
			WHEN (
					tune_raw_elt_solaris_vw.tuneStatus = 'Fail'
					AND tune_raw_elt_solaris_vw.latency > 0
					)
				THEN tune_raw_elt_solaris_vw.latency
			END) tunes
	,cast(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING) extractmonth
	,trunc(cast(to_date(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM') dt_monthly
FROM iptv.tune_raw_elt_solaris_vw
INNER JOIN iptv.IPTV_MAESTRO_STB_AC ON IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL = tune_raw_elt_solaris_vw.accountsourceid
INNER JOIN app_shared_dim.lu_month ON month_id = lu_month.month_id
WHERE IPTV_MAESTRO_STB_AC.INCLUDE_FLG = 'Y'
	AND tune_raw_elt_solaris_vw.tuneStatus IN (
		'Fail'	
		,'Success'
		)
	AND tune_raw_elt_solaris_vw.assetClass IN (
		'IVOD'
		,'CDVR'
		,'Linear'
		,'MDVR'
		,'VOD'
		,'DVR'
		)
	AND from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy') = YEAR(CURRENT_DATE)
 AND tune_raw_elt_solaris_vw.received_date > cast('${hiveconf:RUN_DATE}' AS DATE)	
GROUP BY (
		CASE 
			WHEN trim(tune_raw_elt_solaris_vw.assetClass) IN (
					'VOD'
					,'IVOD'
					)
				THEN 'VOD'
			WHEN trim(tune_raw_elt_solaris_vw.assetClass) IN ('CDVR')
				THEN 'DVR'
			WHEN trim(tune_raw_elt_solaris_vw.assetClass) IN ('Linear')	
				THEN 'Linear'
			END
		) 
	,tune_raw_elt_solaris_vw.tuneStatus
	,tune_raw_elt_solaris_vw.assetClass
	,cast(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yy-MM') AS STRING)
	,trunc(cast(to_date(from_unixtime(CAST(tune_raw_elt_solaris_vw.`TIMESTAMP` / 1000 AS BIGINT), 'yyyy-MM-dd')) AS string), 'MM');
