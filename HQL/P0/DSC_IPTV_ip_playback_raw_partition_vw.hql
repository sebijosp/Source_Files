CREATE OR REPLACE VIEW IPTV.IP_PLAYBACK_RAW_PARTITION_VW AS
WITH REF_TBL AS (
SELECT
JOB_NAME           ,
JOB_TYPE           ,
JOB_DOMAIN         ,
JOB_STATUS         ,
JOB_EXECUTION_RANGE,
IPTV_PIPELINE      ,
IPTV_JOB_NAME      ,
IPTV_SOURCE_TABLE  ,
GREATEST(ETL_UPDT_DT,ETL_INSRT_DT) AS LAST_REF_CHANGE,
COUNT(JOB_NAME) OVER (PARTITION BY IPTV_PIPELINE, JOB_EXECUTION_RANGE) AS PIPELINE_CNT
FROM IPTV.NONETL_IPTV_JOB_RELATN_REF
WHERE JOB_STATUS = 'ACTIVE'
),
REF_FLTR AS (
SELECT
JOB_NAME           ,
JOB_TYPE           ,
JOB_DOMAIN         ,
JOB_STATUS         ,
JOB_EXECUTION_RANGE,
IPTV_PIPELINE      ,
IPTV_JOB_NAME      ,
IPTV_SOURCE_TABLE  ,
PIPELINE_CNT
FROM REF_TBL
WHERE JOB_TYPE   = 'PROFILE'
AND   JOB_DOMAIN = 'S3'
AND   JOB_EXECUTION_RANGE = 'D1'
AND   IPTV_SOURCE_TABLE   = 'IPTV.IP_PLAYBACK_RAW'
),
BDM_CONTROL AS (
SELECT
IPTV_END_CURR_MAX_VALUE,
CTL.JOB_NAME           ,
REF.JOB_EXECUTION_RANGE,
REF.IPTV_PIPELINE      ,
REF.IPTV_JOB_NAME      ,
CTL.LAST_RUN_DATE      ,
REF.PIPELINE_CNT        ,
TO_DATE(CTL.EXECUTION_START_TS) AS EXEC_DT
FROM REF_FLTR FLT
INNER JOIN REF_TBL REF
ON  FLT.IPTV_PIPELINE = REF.IPTV_PIPELINE
AND FLT.JOB_EXECUTION_RANGE = REF.JOB_EXECUTION_RANGE
INNER JOIN IPTV.NONETL_JOB_EXECUTION_CNTRL CTL
ON REF.JOB_NAME = CTL.JOB_NAME
AND CTL.EXECUTION_STATUS = 'SUCCEEDED'
WHERE CTL.ETL_INSRT_DT >= REF.LAST_REF_CHANGE 
),
BDM_CONTROL_CNT AS (
SELECT IPTV_PIPELINE, JOB_EXECUTION_RANGE, EXEC_DT, COUNT(DISTINCT JOB_NAME) AS TOTAL_SUCCESS_CNT
FROM BDM_CONTROL
GROUP BY IPTV_PIPELINE, JOB_EXECUTION_RANGE, EXEC_DT
),
COMPLETED_CYCLES AS (
SELECT
A.JOB_NAME     ,
A.IPTV_JOB_NAME,
MAX(IPTV_END_CURR_MAX_VALUE) AS MAXVAL_OF_LAST_FIN_CYCLE,
MAX(LAST_RUN_DATE)           AS RUNDT_OF_LAST_FIN_CYCLE
FROM BDM_CONTROL_CNT B
INNER JOIN BDM_CONTROL A
ON A.IPTV_PIPELINE = B.IPTV_PIPELINE
AND A.JOB_EXECUTION_RANGE = B.JOB_EXECUTION_RANGE
AND A.EXEC_DT = B.EXEC_DT
WHERE B.TOTAL_SUCCESS_CNT >= A.PIPELINE_CNT
AND A.JOB_NAME in (SELECT JOB_NAME FROM REF_FLTR)
GROUP BY A.JOB_NAME,A.IPTV_JOB_NAME
),
CFW AS (
SELECT 
COALESCE(TO_DATE(CTL.RUNDT_OF_LAST_FIN_CYCLE),TO_DATE(DATE_SUB(CURRENT_DATE(),1))) AS MIN_VALUE,
CURRENT_DATE()                                                                     AS MAX_VALUE
FROM REF_FLTR REF
LEFT OUTER JOIN COMPLETED_CYCLES CTL
ON REF.JOB_NAME = CTL.JOB_NAME
)
SELECT 
R.HEADER.UUID,
R.HEADER.VERSIONNUM,
R.HEADER.`TIMESTAMP`,
R.HEADER.PARTNER,
R.HEADER.HOSTNAME,
R.EVENTTYPE,
R.SESSIONID,
R.STARTTIME,
R.ENDTIME,
R.DEVICE.DEVICEID,
R.DEVICE.DEVICESOURCEID,
R.DEVICE.ACCOUNT,
R.DEVICE.ACCOUNTSOURCEID,
R.DEVICE.APPNAME,
R.DEVICE.APPVERSION,
R.DEVICE.IPADDRESS,
R.DEVICE.ISP,
R.DEVICE.NETWORKLOCATION,
R.DEVICE.USERAGENT,
R.ASSET.PROGRAMID,
R.ASSET.MEDIAGUID,
R.ASSET.PROVIDERID,
R.ASSET.ASSETID,
R.ASSET.ASSETCONTENTID,
R.ASSET.MEDIAID,
R.ASSET.PLATFORMID,
R.ASSET.RECORDINGID,
R.ASSET.STREAMID,
R.ASSET.TITLE,
R.ASSETCLASS,
R.REGULATORYTYPE,
R.SESSIONDURATION,
R.COMPLETIONSTATUS,
R.PLAYBACKTYPE,
R.PLAYBACKSTARTED,
R.LEASEINFO.STARTOFWINDOW,
R.LEASEINFO.ENDOFWINDOW,
R.LEASEINFO.PURCHASETIME,
R.LEASEINFO.LEASELENGTH,
R.LEASEINFO.LEASEID,
R.LEASEINFO.LEASEPRICE,
R.FIRSTASSETPOSITION,
R.LASTASSETPOSITION,
R.DRMLATENCY,
R.OPENLATENCY,
R.TRICKPLAYEVENTS.EVENTKEY,
R.FRAGMENTWARNINGEVENTS.OFFSETS,
R.FRAMERATEMIN,
R.FRAMERATEMAX,
R.BITRATEMIN,
R.BITRATEMAX,
R.ERROREVENTS.OFFSET,
R.ERROREVENTS.MESSAGE,
R.ERROREVENTS.CODE,
R.ERROREVENTS.ERRORTYPE,
R.HDP_FILE_NAME,
R.HDP_CREATE_TS,
R.HDP_UPDATE_TS,
R.RECEIVED_DATE
FROM CFW, IPTV.IP_PLAYBACK_RAW R
WHERE TO_DATE(R.RECEIVED_DATE) >= CFW.MIN_VALUE
AND   TO_DATE(R.RECEIVED_DATE) <  CFW.MAX_VALUE 
;

