set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

INSERT OVERWRITE TABLE IPTV.MONTHLY_TUNE_TYPES PARTITION (DT_MONTHLY)
SELECT 
TUNE_TYPE,
TUNE_STATUS,
ASSET_CLASS,
SUM(LATENCY) LATENCY,
COUNT(TUNES) TUNES,
MAX(extractmonth),
dt_monthly
FROM 
(
  SELECT distinct 
    header.uuid
    ,(CASE 
     WHEN trim(tune_fct.assetClass) IN ('VOD','IVOD')
     THEN 'VOD'
     WHEN trim(tune_fct.assetClass) IN ('CDVR')
     THEN 'DVR'
     WHEN trim(UPPER(tune_fct.assetClass)) like 'LINE%'
     THEN 'Linear'
     ELSE tune_fct.assetClass
     END
     ) TUNE_TYPE
    ,CASE WHEN (UPPER(tune_fct.tuneStatus) like 'FAIL%') THEN 'Fail' Else 'Success' END TUNE_STATUS
    ,CASE WHEN (UPPER(tune_fct.assetClass) like 'LINE%') THEN 'Linear' Else tune_fct.assetClass END ASSET_CLASS
    ,Case When (UPPER(tune_fct.tuneStatus) like 'SUCCES%' and tune_fct.latency >0) then tune_fct.latency 
     When (UPPER(tune_fct.tuneStatus) like 'FAIL%' and tune_fct.latency >0) then tune_fct.latency End AS LATENCY
    ,Case When UPPER(tune_fct.tuneStatus) like 'SUCCES%' and tune_fct.deliveryMedium = 'IP' Then tune_fct.header.uuid  
     When (UPPER(tune_fct.tuneStatus) like 'FAIL%' and tune_fct.deliveryMedium = 'IP' and tune_fct.latency >0) Then tune_fct.header.uuid End TUNES
    ,(cast(date_format(tune_fct.event_date,'yy-MM') AS STRING)) extractmonth
    ,TRUNC(tune_fct.event_date, 'MM') dt_monthly
  FROM 
  IPTV.TUNE_FCT 
  INNER JOIN (SELECT DISTINCT PRIM_RESOURCE_VAL FROM IPTV.IPTV_MAESTRO_STB_AC WHERE SUBSCRIBER_TYPE='IP' AND INCLUDE_FLG='Y') IPTV_MAESTRO_STB_AC 
  on IPTV_MAESTRO_STB_AC.PRIM_RESOURCE_VAL=tune_fct.device.accountsourceid 
  WHERE
  (UPPER(tune_fct.tuneStatus)  like 'FAIL%' OR UPPER(tune_fct.tuneStatus)  like 'SUCCES%')
  AND 
  (UPPER(tune_fct.assetClass)  like '%VOD' OR UPPER(tune_fct.assetClass) like '%DVR%' OR UPPER(tune_fct.assetClass) like 'LINEAR%')
  --AND  TRUNC(tune_fct.event_date,'MM') >= TRUNC('2018-09-01','MM')
  AND  TRUNC(tune_fct.event_date,'MM') >= TRUNC(cast('${hiveconf:RUN_DATE}' AS DATE),'MM') 
  ) tune_fct
GROUP BY
dt_monthly,
TUNE_TYPE,
TUNE_STATUS,
ASSET_CLASS;
