CREATE OR REPLACE VIEW `IPTV`.`redbridge_event_fct_incr_vw` AS WITH REF_TBL AS
  (SELECT `nonetl_iptv_job_relatn_ref`.`job_name`,
          `nonetl_iptv_job_relatn_ref`.`job_type`,
          `nonetl_iptv_job_relatn_ref`.`job_domain`,
          `nonetl_iptv_job_relatn_ref`.`job_status`,
          `nonetl_iptv_job_relatn_ref`.`job_execution_range`,
          `nonetl_iptv_job_relatn_ref`.`iptv_pipeline`,
          `nonetl_iptv_job_relatn_ref`.`iptv_job_name`,
          `nonetl_iptv_job_relatn_ref`.`iptv_source_table`,
          GREATEST(`nonetl_iptv_job_relatn_ref`.`etl_updt_dt`, `nonetl_iptv_job_relatn_ref`.`etl_insrt_dt`) AS `LAST_REF_CHANGE`,
          COUNT(`nonetl_iptv_job_relatn_ref`.`job_name`) OVER (PARTITION BY `nonetl_iptv_job_relatn_ref`.`iptv_pipeline`,
                                                                        `nonetl_iptv_job_relatn_ref`.`job_execution_range`) AS `PIPELINE_CNT`
   FROM `IPTV`.`NONETL_IPTV_JOB_RELATN_REF`
   WHERE `nonetl_iptv_job_relatn_ref`.`job_status` = 'ACTIVE' ),
                       REF_FLTR AS
  (SELECT `ref_tbl`.`job_name`,
          `ref_tbl`.`job_type`,
          `ref_tbl`.`job_domain`,
          `ref_tbl`.`job_status`,
          `ref_tbl`.`job_execution_range`,
          `ref_tbl`.`iptv_pipeline`,
          `ref_tbl`.`iptv_job_name`,
          `ref_tbl`.`iptv_source_table`,
          `ref_tbl`.`pipeline_cnt`
   FROM REF_TBL
   WHERE `ref_tbl`.`job_type` = 'SCORECARD'
     AND `ref_tbl`.`job_domain` = 'HIVE'
     AND `ref_tbl`.`job_execution_range` = 'D1'
     AND `ref_tbl`.`iptv_source_table` = 'IPTV.REDBRIDGE_EVENT_FCT' ),
                       BDM_CONTROL AS
  (SELECT `ctl`.`iptv_end_curr_max_value`,
          `ctl`.`job_name`,
          `ref`.`job_execution_range`,
          `ref`.`iptv_pipeline`,
          `ref`.`iptv_job_name`,
          `ctl`.`last_run_date`,
          `ref`.`pipeline_cnt`,
          TO_DATE(`ctl`.`execution_start_ts`) AS `EXEC_DT`
   FROM REF_FLTR FLT
   INNER JOIN REF_TBL REF ON `flt`.`iptv_pipeline` = `ref`.`iptv_pipeline`
   AND `flt`.`job_execution_range` = `ref`.`job_execution_range`
   INNER JOIN `IPTV`.`NONETL_JOB_EXECUTION_CNTRL` `CTL` ON `ref`.`job_name` = `ctl`.`job_name`
   AND `ctl`.`execution_status` = 'SUCCEEDED'
   WHERE `ctl`.`etl_insrt_dt` >= `ref`.`last_ref_change` ),
                       BDM_CONTROL_CNT AS
  (SELECT `bdm_control`.`iptv_pipeline`,
          `bdm_control`.`job_execution_range`,
          `bdm_control`.`exec_dt`,
          COUNT(DISTINCT `bdm_control`.`job_name`) AS `TOTAL_SUCCESS_CNT`
   FROM BDM_CONTROL
   GROUP BY `bdm_control`.`iptv_pipeline`,
            `bdm_control`.`job_execution_range`,
            `bdm_control`.`exec_dt`),
                       COMPLETED_CYCLES AS
  (SELECT `a`.`job_name`,
          `a`.`iptv_job_name`,
          MAX(`a`.`iptv_end_curr_max_value`) AS `MAXVAL_OF_LAST_FIN_CYCLE`,
          MAX(`a`.`last_run_date`) AS `RUNDT_OF_LAST_FIN_CYCLE`
   FROM BDM_CONTROL_CNT B
   INNER JOIN BDM_CONTROL A ON `a`.`iptv_pipeline` = `b`.`iptv_pipeline`
   AND `a`.`job_execution_range` = `b`.`job_execution_range`
   AND `a`.`exec_dt` = `b`.`exec_dt`
   WHERE `b`.`total_success_cnt` >= `a`.`pipeline_cnt`
     AND `a`.`job_name` IN
       (SELECT `ref_fltr`.`job_name`
        FROM REF_FLTR)
   GROUP BY `a`.`job_name`,
            `a`.`iptv_job_name`),
                       GLGN_CTL AS
  (SELECT `rcf`.`job_name` AS `GLGN_JOB_NAME`,
          CAST(`rcf`.`delta_col_prev_max_value` AS TIMESTAMP) AS `ETL_MIN_VALUE`,
          CAST(`rcf`.`delta_col_curr_max_value` AS TIMESTAMP) AS `ETL_MAX_VALUE`
   FROM REF_FLTR FLT
   INNER JOIN `CFW_HDP`.`GLGN_JOB_EXECUTION` `RCF` ON `rcf`.`job_name` = `flt`.`iptv_job_name`
   WHERE `rcf`.`execution_status` = 'SUCCEEDED' ),
                       CFW AS
  (SELECT `etl`.`glgn_job_name`,
          MIN(TO_DATE(`etl`.`etl_min_value`)) AS `MIN_VALUE`,
          MAX(TO_DATE(`etl`.`etl_max_value`)) AS `MAX_VALUE`,
          MIN(`etl`.`etl_min_value`) AS `MIN_VALUE_TS`,
          MAX(`etl`.`etl_max_value`) AS `MAX_VALUE_TS`
   FROM GLGN_CTL ETL
   LEFT OUTER JOIN COMPLETED_CYCLES CTL ON `etl`.`glgn_job_name` = `ctl`.`iptv_job_name`
   WHERE `etl`.`etl_min_value` >= `ctl`.`maxval_of_last_fin_cycle`
     OR `etl`.`etl_max_value` >= CURRENT_DATE()
   GROUP BY `etl`.`glgn_job_name`),
                       CFW_NRM AS
  (SELECT `cfw`.`glgn_job_name`,
          `cfw`.`min_value_ts`,
          `cfw`.`max_value_ts`,
          date_add(`cfw`.`min_value`, `pe`.`i`) AS `date_value`
   FROM CFW LATERAL VIEW posexplode(split(space(datediff(`cfw`.`max_value`, `cfw`.`min_value`)), ' ')) `pe` AS `i`,
                         `x`),
                       RAW_FILTERED AS
  (SELECT `r`.`header`.`UUID`,
          `r`.`header`.`TIMESTAMP`,
          `r`.`received_date`,
          to_date(from_unixtime(cast(`r`.`header`.`timestamp`/1000 AS bigint))) AS `raw_event_date`,
          `r`.`hdp_create_ts`,
          `r`.`hdp_update_ts`,
          `cfw_nrm`.`min_value_ts`,
          `cfw_nrm`.`max_value_ts`,
          ROW_NUMBER() OVER (PARTITION BY `r`.`header`.`UUID`
                             ORDER BY `r`.`header`.`TIMESTAMP` DESC) AS `RNK`
   FROM `IPTV`.`REDBRIDGE_EVENT_RAW` `R`
   INNER JOIN CFW_NRM ON `r`.`received_date` = `cfw_nrm`.`date_value`
   WHERE `r`.`hdp_create_ts` > `cfw_nrm`.`min_value_ts`
     AND `r`.`hdp_create_ts` <= `cfw_nrm`.`max_value_ts` )
SELECT `f`.`header`.`timestamp`,
       `f`.`header`.`uuid`,
       `f`.`header`.`hostname`,
       `f`.`header`.`money`.`traceid`,
       `f`.`header`.`money`.`spanid`,
       `f`.`header`.`money`.`parentspanid`,
       `f`.`header`.`money`.`spanname`,
       `f`.`header`.`money`.`appname`,
       `f`.`header`.`money`.`starttime`,
       `f`.`header`.`money`.`spanduration`,
       `f`.`header`.`money`.`spansuccess`,
       `f`.`eventtype`,
       `f`.`eventsource`,
       `f`.`provider`,
       `f`.`servicename`,
       `f`.`eventtimestamp`,
       `f`.`partner`,
       `f`.`transactionid`,
       `f`.`accountid`,
       `f`.`xboserviceaccountid`,
       `f`.`paid`,
       `f`.`upsellhold`,
       `f`.`transactionalhold`,
       `f`.`denycharge`,
       `f`.`pinrequired`,
       `f`.`action`,
       `f`.`status`,
       `f`.`responsemessages`,
       `f`.`fromaccount`,
       `f`.`toaccount`,
       `f`.`chargeamount`,
       `f`.`currency`,
       `f`.`serviceactivated`,
       `f`.`plantype`,
       `f`.`enddate`,
       `f`.`description`,
       `f`.`esn`,
       `f`.`hdp_file_name`,
       `f`.`hdp_create_ts`,
       `f`.`hdp_update_ts`,
       `f`.`event_date`,
       `r`.`hdp_create_ts` AS `RAW_HDP_CREATE_TS`,
       `r`.`hdp_update_ts` AS `RAW_HDP_UPDATE_TS`,
       `r`.`min_value_ts` AS `CFW_MIN_VALUE_TS`,
       `r`.`max_value_ts` AS `CFW_MAX_VALUE_TS`
FROM RAW_FILTERED R
INNER JOIN `IPTV`.`REDBRIDGE_EVENT_FCT` `F` ON `f`.`event_date` = `r`.`raw_event_date`
AND `f`.`header`.`UUID` = `r`.`uuid`
WHERE `r`.`rnk` = 1;
