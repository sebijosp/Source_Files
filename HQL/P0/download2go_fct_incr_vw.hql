CREATE OR REPLACE VIEW `iptv`.`download2go_fct_incr_vw` AS WITH REF_TBL AS (
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
WHERE ref_tbl.job_type   = 'SCORECARD'
AND   ref_tbl.job_domain = 'HIVE'
AND   ref_tbl.job_execution_range = 'D1'
AND   ref_tbl.iptv_source_table   = 'IPTV.DOWNLOAD2GO_FCT'
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
GLGN_CTL AS (
SELECT
rcf.job_name AS GLGN_JOB_NAME,
CAST(rcf.delta_col_prev_max_value AS TIMESTAMP) AS ETL_MIN_VALUE,
CAST(rcf.delta_col_curr_max_value AS TIMESTAMP) AS ETL_MAX_VALUE
FROM REF_FLTR FLT
INNER JOIN CFW_HDP.GLGN_JOB_EXECUTION RCF
ON rcf.job_name = flt.iptv_job_name
WHERE rcf.execution_status = 'SUCCEEDED'
),
CFW AS (
SELECT
etl.glgn_job_name,
MIN(TO_DATE(etl.etl_min_value)) AS MIN_VALUE,
MAX(TO_DATE(etl.etl_max_value)) AS MAX_VALUE,
MIN(etl.etl_min_value) AS MIN_VALUE_TS,
MAX(etl.etl_max_value) AS MAX_VALUE_TS,
1 as dummy
FROM GLGN_CTL ETL
LEFT OUTER JOIN COMPLETED_CYCLES CTL
ON etl.glgn_job_name = ctl.iptv_job_name
WHERE etl.etl_min_value >= ctl.maxval_of_last_fin_cycle
OR    etl.etl_max_value >= CURRENT_DATE()
GROUP BY etl.glgn_job_name
)
SELECT
`compact`.`timestamp`,
`compact`.`uuid`,
`compact`.`hostname`,
`compact`.`traceid`,
`compact`.`spanid`,
`compact`.`parentspanid`,
`compact`.`spanname`,
`compact`.`appname`,
`compact`.`starttime`,
`compact`.`spanduration`,
`compact`.`spansuccess`,
`compact`.`partnerid`,
`compact`.`sessionid`,
`compact`.`creationtime`,
`compact`.`starttimes`,
`compact`.`endtime`,
`compact`.`status`,
`compact`.`physicaldeviceid`,
`compact`.`serviceaccountid`,
`compact`.`devicetype`,
`compact`.`devicename`,
`compact`.`deviceversion`,
`compact`.`ipaddress`,
`compact`.`networklocation`,
`compact`.`devicesourceid`,
`compact`.`accountsourceid`,
`compact`.`applicationname`,
`compact`.`applicationversion`,
`compact`.`playername`,
`compact`.`playerversion`,
`compact`.`pluginname`,
`compact`.`pluginversion`,
`compact`.`useragent`,
`compact`.`assetclass`,
`compact`.`regulatoryclass`,
`compact`.`playbacktype`,
`compact`.`mediaguid`,
`compact`.`providerid`,
`compact`.`assetid`,
`compact`.`assetcontentid`,
`compact`.`mediaid`,
`compact`.`platformid`,
`compact`.`recordingid`,
`compact`.`streamid`,
`compact`.`easuri`,
`compact`.`manifesturl`,
`compact`.`clientgeneratedtimestamp`,
`compact`.`clientposttimestamp`,
`compact`.`serverreceivedtimestamp`,
`compact`.`clienttimestamp`,
`compact`.`POSITION`,
`compact`.`eventname`,
`compact`.`sourceid`,
`compact`.`signalid`,
`compact`.`servicezone`,
`compact`.`title`,
`compact`.`totalfragments`,
`compact`.`completedfragments`,
`compact`.`resolution`,
`compact`.`city`,
`compact`.`region`,
`compact`.`postalcode`,
`compact`.`country`,
`compact`.`latitude`,
`compact`.`longitude`,
`compact`.`dma`,
`compact`.`utcoffset`,
`compact`.`hdp_file_name`,
`compact`.`hdp_create_ts`,
`compact`.`hdp_update_ts`,
`compact`.`event_date`
FROM (
SELECT
`f`.`header`.`timestamp`,
`f`.`header`.`uuid`,
`f`.`header`.`hostname`,
`f`.`header`.`money`.`traceid`,
`f`.`header`.`MONEY`.`spanid`,
`f`.`header`.`MONEY`.`parentspanid`,
`f`.`header`.`MONEY`.`spanname`,
`f`.`header`.`MONEY`.`appname`,
`f`.`header`.`MONEY`.`starttime`,
`f`.`header`.`MONEY`.`spanduration`,
`f`.`header`.`MONEY`.`spansuccess`,
`f`.`partnerid`,
`f`.`sessionid`,
`f`.`creationtime`,
`f`.`starttimes`[0] AS `starttimes`,
`f`.`endtime`,
`f`.`status`,
`f`.`device`.`physicaldeviceid`,
`f`.`device`.`serviceaccountid`,
`f`.`device`.`devicetype`,
`f`.`device`.`devicename`,
`f`.`device`.`deviceversion`,
`f`.`device`.`ipaddress`,
`f`.`device`.`networklocation`,
`f`.`device`.`devicesourceid`,
`f`.`device`.`accountsourceid`,
`f`.`application`.`applicationname`,
`f`.`application`.`applicationversion`,
`f`.`application`.`playername`,
`f`.`application`.`playerversion`,
`f`.`application`.`pluginname`,
`f`.`application`.`pluginversion`,
`f`.`application`.`useragent`,
`f`.`asset`.`assetclass`,
`f`.`asset`.`regulatoryclass`,
`f`.`asset`.`playbacktype`,
`f`.`asset`.`mediaguid`,
`f`.`asset`.`providerid`,
`f`.`asset`.`assetid`,
`f`.`asset`.`assetcontentid`,
`f`.`asset`.`mediaid`,
`f`.`asset`.`platformid`,
`f`.`asset`.`recordingid`,
`f`.`asset`.`streamid`,
`f`.`asset`.`easuri`,
`f`.`asset`.`manifesturl`,
`f`.`asset`.`virtualstream`.`timing`.`clientgeneratedtimestamp`,
`f`.`asset`.`virtualstream`.`timing`.`clientposttimestamp`,
`f`.`asset`.`virtualstream`.`timing`.`serverreceivedtimestamp`,
`f`.`asset`.`virtualstream`.`timing`.`clienttimestamp`,
`f`.`asset`.`virtualstream`.`timing`.`POSITION`,
`f`.`asset`.`virtualstream`.`eventname`,
`f`.`asset`.`virtualstream`.`sourceid`,
`f`.`asset`.`virtualstream`.`signalid`,
`f`.`asset`.`virtualstream`.`servicezone`,
`f`.`asset`.`title`,
`f`.`totalfragments`,
`f`.`completedfragments`,
`f`.`resolution`,
`f`.`geolocation`.`city`,
`f`.`geolocation`.`region`,
`f`.`geolocation`.`postalcode`,
`f`.`geolocation`.`country`,
`f`.`geolocation`.`latitude`,
`f`.`geolocation`.`longitude`,
`f`.`geolocation`.`dma`,
`f`.`geolocation`.`utcoffset`,
`f`.`hdp_file_name`,
`f`.`hdp_create_ts`,
`f`.`hdp_update_ts`,
`f`.`event_date`,
1 as dummy
FROM `iptv`.`download2go_fct` `F`
where `f`.`event_date` between  date_sub(current_date, 7) and date_sub(current_date, 1)
) COMPACT
JOIN `cfw` on `compact`.`dummy` =`cfw`.`dummy`
where `compact`.`hdp_create_ts` between `cfw`.`min_value_ts`
and `cfw`.`max_value_ts`;
