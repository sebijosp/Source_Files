set hive.execution.engine=tez;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.stagingdir=/tmp/.hivestage;

CREATE VIEW iptv.ip_playback_raw_elt_solaris_vw AS WITH REF_TBL AS (
SELECT
nonetl_iptv_job_relatn_ref.job_name           ,
nonetl_iptv_job_relatn_ref.job_type           ,
nonetl_iptv_job_relatn_ref.job_domain         ,
nonetl_iptv_job_relatn_ref.job_status         ,
nonetl_iptv_job_relatn_ref.job_execution_range,
nonetl_iptv_job_relatn_ref.iptv_pipeline      ,
nonetl_iptv_job_relatn_ref.iptv_job_name      ,
nonetl_iptv_job_relatn_ref.iptv_source_table  ,
GREATEST(nonetl_iptv_job_relatn_ref.etl_updt_dt,nonetl_iptv_job_relatn_ref.etl_insrt_dt) AS LAST_REF_CHANGE,
COUNT(nonetl_iptv_job_relatn_ref.job_name) OVER (PARTITION BY nonetl_iptv_job_relatn_ref.iptv_pipeline, nonetl_iptv_job_relatn_ref.job_execution_range) AS PIPELINE_CNT
FROM IPTV.NONETL_IPTV_JOB_RELATN_REF
WHERE nonetl_iptv_job_relatn_ref.job_status = 'ACTIVE'
),
REF_FLTR AS (
SELECT
ref_tbl.job_name           ,
ref_tbl.job_type           ,
ref_tbl.job_domain         ,
ref_tbl.job_status         ,
ref_tbl.job_execution_range,
ref_tbl.iptv_pipeline      ,
ref_tbl.iptv_job_name      ,
ref_tbl.iptv_source_table  ,
ref_tbl.pipeline_cnt
FROM REF_TBL
WHERE ref_tbl.job_type   = 'PROFILE'
AND   ref_tbl.job_domain = 'HIVE'
AND   ref_tbl.job_execution_range = 'D7'
AND   ref_tbl.iptv_source_table   = 'IPTV.IP_PLAYBACK_RAW'
),
BDM_CONTROL AS (
SELECT
ctl.iptv_end_curr_max_value,
ctl.job_name           ,
ref.job_execution_range,
ref.iptv_pipeline      ,
ref.iptv_job_name      ,
ctl.last_run_date      ,
ref.pipeline_cnt        ,
TO_DATE(ctl.execution_start_ts) AS EXEC_DT
FROM REF_FLTR FLT
INNER JOIN REF_TBL REF
ON  flt.iptv_pipeline = ref.iptv_pipeline
AND flt.job_execution_range = ref.job_execution_range
INNER JOIN IPTV.NONETL_JOB_EXECUTION_CNTRL CTL
ON ref.job_name = ctl.job_name
AND ctl.execution_status = 'SUCCEEDED'
WHERE ctl.etl_insrt_dt >= ref.last_ref_change
),
BDM_CONTROL_CNT AS (
SELECT bdm_control.iptv_pipeline, bdm_control.job_execution_range, bdm_control.exec_dt, COUNT(DISTINCT bdm_control.job_name) AS TOTAL_SUCCESS_CNT
FROM BDM_CONTROL
GROUP BY bdm_control.iptv_pipeline, bdm_control.job_execution_range, bdm_control.exec_dt
),
COMPLETED_CYCLES AS (
SELECT
a.job_name     ,
a.iptv_job_name,
MAX(a.iptv_end_curr_max_value) AS MAXVAL_OF_LAST_FIN_CYCLE,
MAX(a.last_run_date)           AS RUNDT_OF_LAST_FIN_CYCLE
FROM BDM_CONTROL_CNT B
INNER JOIN BDM_CONTROL A
ON a.iptv_pipeline = b.iptv_pipeline
AND a.job_execution_range = b.job_execution_range
AND a.exec_dt = b.exec_dt
WHERE b.total_success_cnt >= a.pipeline_cnt
AND a.job_name in (SELECT ref_fltr.job_name FROM REF_FLTR)
GROUP BY a.job_name,a.iptv_job_name
),
CFW AS (
SELECT
COALESCE(TO_DATE(DATE_SUB(ctl.rundt_of_last_fin_cycle,7)),TO_DATE(DATE_SUB(CURRENT_DATE(),8))) AS MIN_VALUE,
TO_DATE(DATE_SUB(CURRENT_DATE(),7))                                                            AS MAX_VALUE
FROM REF_FLTR REF
LEFT OUTER JOIN COMPLETED_CYCLES CTL
ON ref.job_name = ctl.job_name
)
SELECT
compact.uuid,
compact.versionnum,
compact.`timestamp`,
compact.partner,
compact.hostname,
compact.eventtype,
compact.sessionid,
compact.starttime,
compact.endtime,
compact.deviceid,
compact.devicesourceid,
compact.account,
compact.accountsourceid,
compact.appname,
compact.appversion,
compact.ipaddress,
compact.isp,
compact.networklocation,
compact.useragent,
compact.programid,
compact.mediaguid,
compact.providerid,
compact.assetid,
compact.assetcontentid,
compact.mediaid,
compact.platformid,
compact.recordingid,
compact.streamid,
compact.title,
compact.assetclass,
compact.regulatorytype,
compact.sessionduration,
compact.completionstatus,
compact.playbacktype,
compact.playbackstarted,
compact.startofwindow,
compact.endofwindow,
compact.purchasetime,
compact.leaselength,
compact.leaseid,
compact.leaseprice,
compact.firstassetposition,
compact.lastassetposition,
compact.drmlatency,
compact.openlatency,
compact.eventkey,
compact.offsets,
compact.frameratemin,
compact.frameratemax,
compact.bitratemin,
compact.bitratemax,
compact.offset,
compact.message,
compact.code,
compact.errortype,
compact.hdp_file_name,
compact.hdp_create_ts,
compact.hdp_update_ts,
compact.received_date
FROM (
SELECT
r.header.UUID,
r.header.VERSIONNUM,
r.header.`TIMESTAMP`,
r.header.PARTNER,
r.header.HOSTNAME,
r.eventtype,
r.sessionid,
r.starttime,
r.endtime,
r.device.DEVICEID,
r.device.DEVICESOURCEID,
r.device.ACCOUNT,
r.device.ACCOUNTSOURCEID,
r.device.APPNAME,
r.device.APPVERSION,
r.device.IPADDRESS,
r.device.ISP,
r.device.NETWORKLOCATION,
r.device.USERAGENT,
r.asset.PROGRAMID,
r.asset.MEDIAGUID,
r.asset.PROVIDERID,
r.asset.ASSETID,
r.asset.ASSETCONTENTID,
r.asset.MEDIAID,
r.asset.PLATFORMID,
r.asset.RECORDINGID,
r.asset.STREAMID,
r.asset.TITLE,
r.assetclass,
r.regulatorytype,
r.sessionduration,
r.completionstatus,
r.playbacktype,
r.playbackstarted,
r.leaseinfo.STARTOFWINDOW,
r.leaseinfo.ENDOFWINDOW,
r.leaseinfo.PURCHASETIME,
r.leaseinfo.LEASELENGTH,
r.leaseinfo.LEASEID,
r.leaseinfo.LEASEPRICE,
r.firstassetposition,
r.lastassetposition,
r.drmlatency,
r.openlatency,
r.trickplayevents.EVENTKEY,
r.fragmentwarningevents.OFFSETS,
r.frameratemin,
r.frameratemax,
r.bitratemin,
r.bitratemax,
r.errorevents.OFFSET,
r.errorevents.MESSAGE,
r.errorevents.CODE,
r.errorevents.ERRORTYPE,
r.hdp_file_name,
r.hdp_create_ts,
r.hdp_update_ts,
r.received_date,
ROW_NUMBER() OVER (PARTITION BY r.header.UUID ORDER BY r.header.`TIMESTAMP` DESC) AS RNK,
TO_DATE(FROM_UNIXTIME(CAST(r.header.`TIMESTAMP`/1000 AS BIGINT))) AS RAW_EVENT_DATE
FROM CFW, IPTV.IP_PLAYBACK_RAW R
WHERE UPPER(r.eventtype) = 'END'
--AND TO_DATE(FROM_UNIXTIME(CAST(r.header.`TIMESTAMP`/1000 AS BIGINT))) >= cfw.min_value
--AND TO_DATE(FROM_UNIXTIME(CAST(r.header.`TIMESTAMP`/1000 AS BIGINT))) <  cfw.max_value
) COMPACT
WHERE compact.rnk = 1
AND compact.raw_event_date >= TO_DATE(DATE_SUB(CURRENT_DATE(),30))
AND (compact.providerid IS NULL OR UPPER(compact.providerid) NOT LIKE 'PPV_%');
