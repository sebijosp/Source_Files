CREATE OR REPLACE VIEW `iptv`.`ip_playback_raw_incr_vw` AS WITH ref_tbl AS
  (SELECT `nonetl_iptv_job_relatn_ref`.`job_name`,
          `nonetl_iptv_job_relatn_ref`.`job_type`,
          `nonetl_iptv_job_relatn_ref`.`job_domain`,
          `nonetl_iptv_job_relatn_ref`.`job_status`,
          `nonetl_iptv_job_relatn_ref`.`job_execution_range`,
          `nonetl_iptv_job_relatn_ref`.`iptv_pipeline`,
          `nonetl_iptv_job_relatn_ref`.`iptv_job_name`,
          `nonetl_iptv_job_relatn_ref`.`iptv_source_table`,
          greatest(`nonetl_iptv_job_relatn_ref`.`etl_updt_dt`, `nonetl_iptv_job_relatn_ref`.`etl_insrt_dt`) AS `last_ref_change`,
          count(`nonetl_iptv_job_relatn_ref`.`job_name`) OVER (PARTITION BY `nonetl_iptv_job_relatn_ref`.`iptv_pipeline`,
                                                                            `nonetl_iptv_job_relatn_ref`.`job_execution_range`) AS `pipeline_cnt`
   FROM `iptv`.`nonetl_iptv_job_relatn_ref`
   WHERE `nonetl_iptv_job_relatn_ref`.`job_status` = 'ACTIVE' ) ,
            ref_fltr AS
  (SELECT `ref_tbl`.`job_name`,
          `ref_tbl`.`job_type`,
          `ref_tbl`.`job_domain`,
          `ref_tbl`.`job_status`,
          `ref_tbl`.`job_execution_range`,
          `ref_tbl`.`iptv_pipeline`,
          `ref_tbl`.`iptv_job_name`,
          `ref_tbl`.`iptv_source_table`,
          `ref_tbl`.`pipeline_cnt`
   FROM ref_tbl
   WHERE `ref_tbl`.`job_type` = 'SCORECARD'
     AND `ref_tbl`.`job_domain` = 'HIVE'
     AND `ref_tbl`.`job_execution_range` = 'D1'
     AND `ref_tbl`.`iptv_source_table` = 'IPTV.IP_PLAYBACK_RAW' ) ,
            bdm_control AS
  (SELECT `ctl`.`iptv_end_curr_max_value`,
          `ctl`.`job_name`,
          `ref`.`job_execution_range`,
          `ref`.`iptv_pipeline`,
          `ref`.`iptv_job_name`,
          `ctl`.`last_run_date`,
          `ref`.`pipeline_cnt`,
          to_date(`ctl`.`execution_start_ts`) AS `exec_dt`
   FROM ref_fltr flt
   INNER JOIN ref_tbl REF ON `flt`.`iptv_pipeline` = `ref`.`iptv_pipeline`
   AND `flt`.`job_execution_range` = `ref`.`job_execution_range`
   INNER JOIN `iptv`.`nonetl_job_execution_cntrl` `ctl` ON `ref`.`job_name` = `ctl`.`job_name`
   AND `ctl`.`execution_status` = 'SUCCEEDED'
   WHERE `ctl`.`etl_insrt_dt` >= `ref`.`last_ref_change` ) ,
            bdm_control_cnt AS
  (SELECT `bdm_control`.`iptv_pipeline`,
          `bdm_control`.`job_execution_range`,
          `bdm_control`.`exec_dt`,
          count(DISTINCT `bdm_control`.`job_name`) AS `total_success_cnt`
   FROM bdm_control
   GROUP BY `bdm_control`.`iptv_pipeline`,
            `bdm_control`.`job_execution_range`,
            `bdm_control`.`exec_dt`) ,
            completed_cycles AS
  (SELECT `a`.`job_name`,
          `a`.`iptv_job_name`,
          max(`a`.`iptv_end_curr_max_value`) AS `maxval_of_last_fin_cycle`,
          max(`a`.`last_run_date`) AS `rundt_of_last_fin_cycle`
   FROM bdm_control_cnt b
   INNER JOIN bdm_control a ON `a`.`iptv_pipeline` = `b`.`iptv_pipeline`
   AND `a`.`job_execution_range` = `b`.`job_execution_range`
   AND `a`.`exec_dt` = `b`.`exec_dt`
   WHERE `b`.`total_success_cnt` >= `a`.`pipeline_cnt`
     AND `a`.`job_name` IN
       (SELECT `ref_fltr`.`job_name`
        FROM ref_fltr)
   GROUP BY `a`.`job_name`,
            `a`.`iptv_job_name`) ,
            glgn_ctl AS
  (SELECT `rcf`.`job_name` AS `glgn_job_name`,
          cast(`rcf`.`delta_col_prev_max_value` AS TIMESTAMP) AS `etl_min_value`,
          cast(`rcf`.`delta_col_curr_max_value` AS TIMESTAMP) AS `etl_max_value`
   FROM ref_fltr flt
   INNER JOIN `cfw_hdp`.`glgn_job_execution` `rcf` ON `rcf`.`job_name` = `flt`.`iptv_job_name`
   WHERE `rcf`.`execution_status` = 'SUCCEEDED' ) ,
            cfw AS
  (SELECT `etl`.`glgn_job_name`,
          min(to_date(`etl`.`etl_min_value`)) AS `min_value`,
          max(to_date(`etl`.`etl_max_value`)) AS `max_value`,
          min(`etl`.`etl_min_value`) AS `min_value_ts`,
          max(`etl`.`etl_max_value`) AS `max_value_ts`
   FROM glgn_ctl etl
   LEFT OUTER JOIN completed_cycles ctl ON `etl`.`glgn_job_name` = `ctl`.`iptv_job_name`
   WHERE `etl`.`etl_min_value` >= `ctl`.`maxval_of_last_fin_cycle`
     OR `etl`.`etl_max_value` >= CURRENT_DATE()
   GROUP BY `etl`.`glgn_job_name`) ,
            cfw_nrm AS
  (SELECT `cfw`.`glgn_job_name`,
          `cfw`.`min_value_ts`,
          `cfw`.`max_value_ts`,
          date_add(`cfw`.`min_value`, `pe`.`i`) AS `date_value`
   FROM cfw LATERAL VIEW POSEXPLODE(split(space(datediff(`cfw`.`max_value`, `cfw`.`min_value`)), ' ')) `pe` AS `i`,
                                   `x`)
SELECT `r`.`header`.`uuid`,
          `r`.`header`.`versionnum`,
          `r`.`header`.`timestamp`,
          `r`.`header`.`partner`,
          `r`.`header`.`hostname`,
          `r`.`eventtype`,
          `r`.`sessionid`,
          `r`.`starttime`,
          `r`.`endtime`,
          `r`.`device`.`deviceid`,
          `r`.`device`.`devicesourceid`,
          `r`.`device`.`account`,
          `r`.`device`.`accountsourceid`,
          `r`.`device`.`appname`,
          `r`.`device`.`appversion`,
          `r`.`device`.`ipaddress`,
          `r`.`device`.`isp`,
          `r`.`device`.`networklocation`,
          `r`.`device`.`useragent`,
          `r`.`asset`.`programid`,
          `r`.`asset`.`mediaguid`,
          `r`.`asset`.`providerid`,
          `r`.`asset`.`assetid`,
          `r`.`asset`.`assetcontentid`,
          `r`.`asset`.`mediaid`,
          `r`.`asset`.`platformid`,
          `r`.`asset`.`recordingid`,
          `r`.`asset`.`streamid`,
          `r`.`asset`.`title`,
          `r`.`assetclass`,
          `r`.`regulatorytype`,
          `r`.`sessionduration`,
          `r`.`completionstatus`,
          `r`.`playbacktype`,
          `r`.`playbackstarted`,
          `r`.`leaseinfo`.`startofwindow`,
          `r`.`leaseinfo`.`endofwindow`,
          `r`.`leaseinfo`.`purchasetime`,
          `r`.`leaseinfo`.`leaselength`,
          `r`.`leaseinfo`.`leaseid`,
          `r`.`leaseinfo`.`leaseprice`,
          `r`.`firstassetposition`,
          `r`.`lastassetposition`,
          `r`.`drmlatency`,
          `r`.`openlatency`,
          `r`.`frameratemin`,
          `r`.`frameratemax`,
          `r`.`bitratemin`,
          `r`.`bitratemax`,
          `r`.`errorevents`.`offset`,
          `r`.`errorevents`.`message`,
          `r`.`errorevents`.`code`,
          `r`.`errorevents`.`errortype`,
          `r`.`hdp_file_name`,
          `r`.`hdp_create_ts`,
          `r`.`hdp_update_ts`,
          `r`.`received_date`
   FROM `iptv`.`ip_playback_raw` `r`
   JOIN cfw_nrm ON `r`.`received_date` = `cfw_nrm`.`date_value`
   WHERE `r`.`hdp_create_ts` > `cfw_nrm`.`min_value_ts`
     AND `r`.`hdp_create_ts` <= `cfw_nrm`.`max_value_ts` 
