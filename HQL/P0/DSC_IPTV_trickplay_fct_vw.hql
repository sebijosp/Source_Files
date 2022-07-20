CREATE OR REPLACE VIEW `iptv.trickplay_fct_vw`
             AS
WITH REF_TBL AS (SELECT`nonetl_iptv_job_relatn_ref`.`job_name` ,`nonetl_iptv_job_relatn_ref`.`job_type` ,`nonetl_iptv_job_relatn_ref`.`job_domain` ,`nonetl_iptv_job_relatn_ref`.`job_status` ,`nonetl_iptv_job_relatn_ref`.`job_execution_range`,`nonetl_iptv_job_relatn_ref`.`iptv_pipeline` ,`nonetl_iptv_job_relatn_ref`.`iptv_job_name` ,`nonetl_iptv_job_relatn_ref`.`iptv_source_table` ,GREATEST(`nonetl_iptv_job_relatn_ref`.`etl_updt_dt`,`nonetl_iptv_job_relatn_ref`.`etl_insrt_dt`) AS `LAST_REF_CHANGE`,COUNT(`nonetl_iptv_job_relatn_ref`.`job_name`) OVER (PARTITION BY `nonetl_iptv_job_relatn_ref`.`iptv_pipeline`, `nonetl_iptv_job_relatn_ref`.`job_execution_range`) AS `PIPELINE_CNT`FROM `IPTV`.`NONETL_IPTV_JOB_RELATN_REF`WHERE `nonetl_iptv_job_relatn_ref`.`job_status` = 'ACTIVE'),
  REF_FLTR   AS (SELECT`ref_tbl`.`job_name` ,`ref_tbl`.`job_type` ,`ref_tbl`.`job_domain` ,`ref_tbl`.`job_status` ,`ref_tbl`.`job_execution_range`,`ref_tbl`.`iptv_pipeline` ,`ref_tbl`.`iptv_job_name` ,`ref_tbl`.`iptv_source_table` ,`ref_tbl`.`pipeline_cnt`FROM REF_TBL
WHERE `ref_tbl`.`job_type`          = 'PROFILE'
AND `ref_tbl`.`job_domain`          = 'HIVE'
AND `ref_tbl`.`job_execution_range` = 'D7'
AND `ref_tbl`.`iptv_source_table`   = 'IPTV.TRICKPLAY_FCT'),
  BDM_CONTROL AS (SELECT`ctl`.`iptv_end_curr_max_value`,`ctl`.`job_name` ,`ref`.`job_execution_range`,`ref`.`iptv_pipeline` ,`ref`.`iptv_job_name` ,`ctl`.`last_run_date` ,`ref`.`pipeline_cnt` ,TO_DATE(`ctl`.`execution_start_ts`) AS `EXEC_DT`
FROM REF_FLTR FLT
INNER JOIN REF_TBL REF
ON `flt`.`iptv_pipeline`                                          = `ref`.`iptv_pipeline`AND `flt`.`job_execution_range` = `ref`.`job_execution_range`INNER
JOIN `IPTV`.`NONETL_JOB_EXECUTION_CNTRL` `CTL`ON `ref`.`job_name` = `ctl`.`job_name`AND `ctl`.`execution_status` = 'SUCCEEDED'
WHERE `ctl`.`etl_insrt_dt`                                       >= `ref`.`last_ref_change` ),
  BDM_CONTROL_CNT AS
  (SELECT `bdm_control`.`iptv_pipeline`,
    `bdm_control`.`job_execution_range`,
    `bdm_control`.`exec_dt`,
    COUNT(DISTINCT `bdm_control`.`job_name`) AS `TOTAL_SUCCESS_CNT`FROM BDM_CONTROL
  GROUP BY `bdm_control`.`iptv_pipeline`,
    `bdm_control`.`job_execution_range`,
    `bdm_control`.`exec_dt`
  ),
  COMPLETED_CYCLES AS (SELECT`a`.`job_name` ,`a`.`iptv_job_name`,MAX(`a`.`iptv_end_curr_max_value`) AS `MAXVAL_OF_LAST_FIN_CYCLE`,MAX(`a`.`last_run_date`) AS `RUNDT_OF_LAST_FIN_CYCLE`FROM BDM_CONTROL_CNT B
INNER JOIN BDM_CONTROL A
ON `a`.`iptv_pipeline` = `b`.`iptv_pipeline`AND `a`.`job_execution_range` = `b`.`job_execution_range`AND `a`.`exec_dt` = `b`.`exec_dt`WHERE `b`.`total_success_cnt` >= `a`.`pipeline_cnt`AND `a`.`job_name` IN
  (SELECT `ref_fltr`.`job_name` FROM REF_FLTR
  )
GROUP BY `a`.`job_name`,`a`.`iptv_job_name`),
  CFW AS
  (SELECT COALESCE(TO_DATE(DATE_SUB(`ctl`.`rundt_of_last_fin_cycle`,7)),TO_DATE(DATE_SUB(CURRENT_DATE(),8))) AS `MIN_VALUE`,
    TO_DATE(DATE_SUB(CURRENT_DATE(),7))                                                                      AS `MAX_VALUE`FROM REF_FLTR REF
  LEFT OUTER JOIN COMPLETED_CYCLES CTL
  ON `ref`.`job_name` = `ctl`.`job_name`
  )
SELECT `f`.`header`.`TIMESTAMP`,
  `f`.`header`.`UUID`,
  `f`.`header`.`HOSTNAME`,
  `f`.`header`.`MONEY`.`TRACEID`,
  `f`.`header`.`MONEY`.`SPANID`,
  `f`.`header`.`MONEY`.`PARENTSPANID`,
  `f`.`header`.`MONEY`.`SPANNAME`,
  `f`.`header`.`MONEY`.`APPNAME`,
  `f`.`header`.`MONEY`.`STARTTIME`,
  `f`.`header`.`MONEY`.`SPANDURATION`,
  `f`.`header`.`MONEY`.`SPANSUCCESS`,
  `f`.`header`.`MONEY`.`NOTES`,
  `f`.`device`.`RECEIVERID`,
  `f`.`device`.`DEVICEID`,
  `f`.`device`.`DEVICESOURCEID`,
  `f`.`device`.`ACCOUNT`,
  `f`.`device`.`ACCOUNTSOURCEID`,
  `f`.`device`.`BILLINGACCOUNTID`,
  `f`.`device`.`MACADDRESS`,
  `f`.`device`.`ECMMACADDRESS`,
  `f`.`device`.`FIRMWAREVERSION`,
  `f`.`device`.`DEVICETYPE`,
  `f`.`device`.`MAKE`,
  `f`.`device`.`MODEL`,
  `f`.`device`.`PARTNER`,
  `f`.`device`.`IPADDRESS`,
  `f`.`device`.`UTCOFFSET`,
  `f`.`device`.`POSTALCODE`,
  `f`.`eventtimestamp`,
  `f`.`trickplayname`,
  `f`.`playbackrate`,
  `f`.`assetclass`,
  `f`.`assetposition`,
  `f`.`hdp_file_name`,
  `f`.`hdp_create_ts`,
  `f`.`hdp_update_ts`,
  `f`.`event_date`FROM CFW,
  `IPTV`.`TRICKPLAY_FCT` `F`WHERE TO_DATE(`f`.`event_date`) >= `cfw`.`min_value`AND TO_DATE(`f`.`event_date`) < `cfw`.`max_value`;
