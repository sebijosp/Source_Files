CREATE OR REPLACE VIEW `iptv`.`dvr_schedule_raw_partition_vw` AS WITH REF_TBL AS (
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
WHERE `ref_tbl`.`job_type`   = 'PROFILE'
AND   `ref_tbl`.`job_domain` = 'S3'
AND   `ref_tbl`.`job_execution_range` = 'D1'
AND   `ref_tbl`.`iptv_source_table`   = 'IPTV.DVR_SCHEDULE_RAW'
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
CFW AS (
SELECT
COALESCE(TO_DATE(`ctl`.`rundt_of_last_fin_cycle`),TO_DATE(DATE_SUB(CURRENT_DATE(),1))) AS `MIN_VALUE`,
CURRENT_DATE()                                                                     AS `MAX_VALUE`
FROM REF_FLTR REF
LEFT OUTER JOIN COMPLETED_CYCLES CTL
ON `ref`.`job_name` = `ctl`.`job_name`
),
CFW_NRM as (
SELECT 
date_add(`cfw`.`min_value`,`pe`.`i`)  as `date_value`  from CFW
lateral view
posexplode(split(space(datediff(`cfw`.`max_value`,`cfw`.`min_value`)),' ')) `pe` as `i`,`x`
)
SELECT
`r`.`header`.`TIMESTAMP` ,
`r`.`header`.`UUID` ,
`r`.`header`.`HOSTNAME` ,
`r`.`header`.`MONEY`.`TRACEID` ,
`r`.`header`.`MONEY`.`SPANID` ,
`r`.`header`.`MONEY`.`PARENTSPANID` ,
`r`.`header`.`MONEY`.`SPANNAME` ,
`r`.`header`.`MONEY`.`APPNAME` ,
`r`.`header`.`MONEY`.`STARTTIME` ,
`r`.`header`.`MONEY`.`SPANDURATION` ,
`r`.`header`.`MONEY`.`SPANSUCCESS` ,
`r`.`partner` ,
`r`.`eventtimestamp` ,
`r`.`description` ,
`r`.`device`.`RECEIVERID` ,
`r`.`device`.`DEVICEID` ,
`r`.`device`.`DEVICESOURCEID` ,
`r`.`device`.`ACCOUNT` ,
`r`.`device`.`ACCOUNTSOURCEID` ,
`r`.`device`.`MACADDRESS` ,
`r`.`device`.`ECMMACADDRESS` ,
`r`.`device`.`FIRMWAREVERSION` ,
`r`.`device`.`DEVICETYPE` ,
`r`.`device`.`PARTNER` AS `DEVICE_PARTNER`,
`r`.`device`.`IPADDRESS` ,
`r`.`device`.`UTCOFFSET` ,
`r`.`fullschedule` ,
`r`.`hdp_file_name` ,
`r`.`hdp_create_ts` ,
`r`.`hdp_update_ts`,
`r`.`received_date`
FROM `IPTV`.`DVR_SCHEDULE_RAW` `R`
JOIN CFW_NRM ON `r`.`received_date` = `cfw_nrm`.`date_value`;
