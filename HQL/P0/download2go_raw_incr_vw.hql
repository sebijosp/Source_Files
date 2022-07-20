CREATE OR REPLACE VIEW `iptv`.`download2go_raw_incr_vw` AS WITH REF_TBL AS (
SELECT
`nonetl_iptv_job_relatn_ref`.`job_name`           ,
`nonetl_iptv_job_relatn_ref`.`job_type`           ,
`nonetl_iptv_job_relatn_ref`.`job_domain`         ,
`nonetl_iptv_job_relatn_ref`.`job_status`         ,
`nonetl_iptv_job_relatn_ref`.`job_execution_range`,
`nonetl_iptv_job_relatn_ref`.`iptv_pipeline`      ,
`nonetl_iptv_job_relatn_ref`.`iptv_job_name`      ,
`nonetl_iptv_job_relatn_ref`.`iptv_source_table`  ,
GREATEST(`nonetl_iptv_job_relatn_ref`.`etl_updt_dt`,`nonetl_iptv_job_relatn_ref`.`etl_insrt_dt`) AS `LAST_REF_CHANGE`,
COUNT(`nonetl_iptv_job_relatn_ref`.`job_name`) OVER (PARTITION BY `nonetl_iptv_job_relatn_ref`.`iptv_pipeline`, `nonetl_iptv_job_relatn_ref`.`job_execution_range`) AS `PIPELINE_CNT`
FROM `IPTV`.`NONETL_IPTV_JOB_RELATN_REF`
WHERE `nonetl_iptv_job_relatn_ref`.`job_status` = 'ACTIVE'
),
REF_FLTR AS (
SELECT
`ref_tbl`.`job_name`           ,
`ref_tbl`.`job_type`           ,
`ref_tbl`.`job_domain`         ,
`ref_tbl`.`job_status`         ,
`ref_tbl`.`job_execution_range`,
`ref_tbl`.`iptv_pipeline`      ,
`ref_tbl`.`iptv_job_name`      ,
`ref_tbl`.`iptv_source_table`  ,
`ref_tbl`.`pipeline_cnt`
FROM REF_TBL
WHERE `ref_tbl`.`job_type`   = 'SCORECARD'
AND   `ref_tbl`.`job_domain` = 'HIVE'
AND   `ref_tbl`.`job_execution_range` = 'D1'
AND   `ref_tbl`.`iptv_source_table`   = 'IPTV.DOWNLOAD2GO_RAW'
),
BDM_CONTROL AS (
SELECT
`ctl`.`iptv_end_curr_max_value`,
`ctl`.`job_name`           ,
`ref`.`job_execution_range`,
`ref`.`iptv_pipeline`      ,
`ref`.`iptv_job_name`      ,
`ctl`.`last_run_date`      ,
`ref`.`pipeline_cnt`        ,
TO_DATE(`ctl`.`execution_start_ts`) AS `EXEC_DT`
FROM REF_FLTR FLT
INNER JOIN REF_TBL REF
ON  `flt`.`iptv_pipeline` = `ref`.`iptv_pipeline`
AND `flt`.`job_execution_range` = `ref`.`job_execution_range`
INNER JOIN `IPTV`.`NONETL_JOB_EXECUTION_CNTRL` `CTL`
ON `ref`.`job_name` = `ctl`.`job_name`
AND `ctl`.`execution_status` = 'SUCCEEDED'
WHERE `ctl`.`etl_insrt_dt` >= `ref`.`last_ref_change`
),
BDM_CONTROL_CNT AS (
SELECT `bdm_control`.`iptv_pipeline`, `bdm_control`.`job_execution_range`, `bdm_control`.`exec_dt`, COUNT(DISTINCT `bdm_control`.`job_name`) AS `TOTAL_SUCCESS_CNT`
FROM BDM_CONTROL
GROUP BY `bdm_control`.`iptv_pipeline`, `bdm_control`.`job_execution_range`, `bdm_control`.`exec_dt`
),
COMPLETED_CYCLES AS (
SELECT
`a`.`job_name`     ,
`a`.`iptv_job_name`,
MAX(`a`.`iptv_end_curr_max_value`) AS `MAXVAL_OF_LAST_FIN_CYCLE`,
MAX(`a`.`last_run_date`)           AS `RUNDT_OF_LAST_FIN_CYCLE`
FROM BDM_CONTROL_CNT B
INNER JOIN BDM_CONTROL A
ON `a`.`iptv_pipeline` = `b`.`iptv_pipeline`
AND `a`.`job_execution_range` = `b`.`job_execution_range`
AND `a`.`exec_dt` = `b`.`exec_dt`
WHERE `b`.`total_success_cnt` >= `a`.`pipeline_cnt`
AND `a`.`job_name` in (SELECT `ref_fltr`.`job_name` FROM REF_FLTR)
GROUP BY `a`.`job_name`,`a`.`iptv_job_name`
),
GLGN_CTL AS (
SELECT
`rcf`.`job_name` AS `GLGN_JOB_NAME`,
CAST(`rcf`.`delta_col_prev_max_value` AS TIMESTAMP) AS `ETL_MIN_VALUE`,
CAST(`rcf`.`delta_col_curr_max_value` AS TIMESTAMP) AS `ETL_MAX_VALUE`
FROM REF_FLTR FLT
INNER JOIN `CFW_HDP`.`GLGN_JOB_EXECUTION` `RCF`
ON `rcf`.`job_name` = `flt`.`iptv_job_name`
WHERE `rcf`.`execution_status` = 'SUCCEEDED'
),
CFW AS (
SELECT
`etl`.`glgn_job_name`,
MIN(TO_DATE(`etl`.`etl_min_value`)) AS `MIN_VALUE`,
MAX(TO_DATE(`etl`.`etl_max_value`)) AS `MAX_VALUE`,
MIN(`etl`.`etl_min_value`) AS `MIN_VALUE_TS`,
MAX(`etl`.`etl_max_value`) AS `MAX_VALUE_TS`
FROM GLGN_CTL ETL
LEFT OUTER JOIN COMPLETED_CYCLES CTL
ON `etl`.`glgn_job_name` = `ctl`.`iptv_job_name`
WHERE `etl`.`etl_min_value` >= `ctl`.`maxval_of_last_fin_cycle`
OR    `etl`.`etl_max_value` >= CURRENT_DATE()
GROUP BY `etl`.`glgn_job_name`
),
CFW_NRM as (
SELECT `cfw`.`glgn_job_name`, `cfw`.`min_value_ts`, `cfw`.`max_value_ts`,
date_add(`cfw`.`min_value`,`pe`.`i`)  as `date_value`  from CFW
lateral view
        posexplode(split(space(datediff(`cfw`.`max_value`,`cfw`.`min_value`)),' ')) `pe` as `i`,`x`
)
SELECT 
`r`.`header`.`timestamp`,
`r`.`header`.`uuid`,
`r`.`header`.`hostname`,
`r`.`header`.`money`.`traceid`,
`r`.`header`.`MONEY`.`spanid`,
`r`.`header`.`MONEY`.`parentspanid`,
`r`.`header`.`MONEY`.`spanname`,
`r`.`header`.`MONEY`.`appname`,
`r`.`header`.`MONEY`.`starttime`,
`r`.`header`.`MONEY`.`spanduration`,
`r`.`header`.`MONEY`.`spansuccess`,
`r`.`partnerid`,
`r`.`sessionid`,
`r`.`creationtime`,
`r`.`starttimes`[0] AS `starttimes`,
`r`.`endtime`,
`r`.`status`,
`r`.`device`.`physicaldeviceid`,
`r`.`device`.`serviceaccountid`,
`r`.`device`.`devicetype`,
`r`.`device`.`devicename`,
`r`.`device`.`deviceversion`,
`r`.`device`.`ipaddress`,
`r`.`device`.`networklocation`,
`r`.`device`.`devicesourceid`,
`r`.`device`.`accountsourceid`,
`r`.`application`.`applicationname`,
`r`.`application`.`applicationversion`,
`r`.`application`.`playername`,
`r`.`application`.`playerversion`,
`r`.`application`.`pluginname`,
`r`.`application`.`pluginversion`,
`r`.`application`.`useragent`,
`r`.`asset`.`assetclass`,
`r`.`asset`.`regulatoryclass`,
`r`.`asset`.`playbacktype`,
`r`.`asset`.`mediaguid`,
`r`.`asset`.`providerid`,
`r`.`asset`.`assetid`,
`r`.`asset`.`assetcontentid`,
`r`.`asset`.`mediaid`,
`r`.`asset`.`platformid`,
`r`.`asset`.`recordingid`,
`r`.`asset`.`streamid`,
`r`.`asset`.`easuri`,
`r`.`asset`.`manifesturl`,
`r`.`asset`.`virtualstream`.`timing`.`clientgeneratedtimestamp`,
`r`.`asset`.`virtualstream`.`timing`.`clientposttimestamp`,
`r`.`asset`.`virtualstream`.`timing`.`serverreceivedtimestamp`,
`r`.`asset`.`virtualstream`.`timing`.`clienttimestamp`,
`r`.`asset`.`virtualstream`.`timing`.`POSITION`,
`r`.`asset`.`virtualstream`.`eventname`,
`r`.`asset`.`virtualstream`.`sourceid`,
`r`.`asset`.`virtualstream`.`signalid`,
`r`.`asset`.`virtualstream`.`servicezone`,
`r`.`asset`.`title`,
`r`.`totalfragments`,
`r`.`completedfragments`,
`r`.`resolution`,
`r`.`geolocation`.`city`,
`r`.`geolocation`.`region`,
`r`.`geolocation`.`postalcode`,
`r`.`geolocation`.`country`,
`r`.`geolocation`.`latitude`,
`r`.`geolocation`.`longitude`,
`r`.`geolocation`.`dma`,
`r`.`geolocation`.`utcoffset`,
`r`.`hdp_file_name`,
`r`.`hdp_create_ts`,
`r`.`hdp_update_ts`,
`r`.`received_date`
from `iptv`.`download2go_raw` `R`
JOIN CFW_NRM ON `r`.`received_date` = `cfw_nrm`.`date_value`
WHERE `r`.`hdp_create_ts` >  `cfw_nrm`.`min_value_ts`
AND `r`.`hdp_create_ts` <= `cfw_nrm`.`max_value_ts`;
