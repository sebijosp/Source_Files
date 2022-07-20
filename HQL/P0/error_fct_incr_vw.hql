CREATE OR REPLACE VIEW `iptv`.`error_fct_incr_vw` AS WITH ref_tbl             AS
                       (
                              SELECT `nonetl_iptv_job_relatn_ref`.`job_name` ,
                                     `nonetl_iptv_job_relatn_ref`.`job_type` ,
                                     `nonetl_iptv_job_relatn_ref`.`job_domain` ,
                                     `nonetl_iptv_job_relatn_ref`.`job_status` ,
                                     `nonetl_iptv_job_relatn_ref`.`job_execution_range`,
                                     `nonetl_iptv_job_relatn_ref`.`iptv_pipeline` ,
                                     `nonetl_iptv_job_relatn_ref`.`iptv_job_name` ,
                                     `nonetl_iptv_job_relatn_ref`.`iptv_source_table` ,
                                     greatest(`nonetl_iptv_job_relatn_ref`.`etl_updt_dt`,`nonetl_iptv_job_relatn_ref`.`etl_insrt_dt`)                                                                    AS `last_ref_change`,
                                     count(`nonetl_iptv_job_relatn_ref`.`job_name`) OVER (partition BY `nonetl_iptv_job_relatn_ref`.`iptv_pipeline`, `nonetl_iptv_job_relatn_ref`.`job_execution_range`) AS `pipeline_cnt`
                              FROM   `iptv`.`nonetl_iptv_job_relatn_ref`
                              WHERE  `nonetl_iptv_job_relatn_ref`.`job_status` = 'ACTIVE'
                       )
                       ,
                       ref_fltr AS
                       (
                              SELECT`ref_tbl`.`job_name` ,
                                     `ref_tbl`.`job_type` ,
                                     `ref_tbl`.`job_domain` ,
                                     `ref_tbl`.`job_status` ,
                                     `ref_tbl`.`job_execution_range`,
                                     `ref_tbl`.`iptv_pipeline` ,
                                     `ref_tbl`.`iptv_job_name` ,
                                     `ref_tbl`.`iptv_source_table` ,
                                     `ref_tbl`.`pipeline_cnt`
                              FROM   ref_tbl
                              WHERE  `ref_tbl`.`job_type` = 'PROFILE'
                              AND    `ref_tbl`.`job_domain` = 'HIVE'
                              AND    `ref_tbl`.`job_execution_range` = 'D1'
                              AND    `ref_tbl`.`iptv_source_table` = 'IPTV.ERROR_FCT'
                       )
                       ,
                       bdm_control AS
                       (
                                  SELECT     `ctl`.`iptv_end_curr_max_value`,
                                             `ctl`.`job_name` ,
                                             `ref`.`job_execution_range`,
                                             `ref`.`iptv_pipeline` ,
                                             `ref`.`iptv_job_name` ,
                                             `ctl`.`last_run_date` ,
                                             `ref`.`pipeline_cnt` ,
                                             to_date(`ctl`.`execution_start_ts`) AS `exec_dt`
                                  FROM       ref_fltr flt
                                  INNER JOIN ref_tbl ref
                                  ON         `flt`.`iptv_pipeline` = `ref`.`iptv_pipeline`
                                  AND        `flt`.`job_execution_range` = `ref`.`job_execution_range`
                                  INNER JOIN `iptv`.`nonetl_job_execution_cntrl` `ctl`
                                  ON         `ref`.`job_name` = `ctl`.`job_name`
                                  AND        `ctl`.`execution_status` = 'SUCCEEDED'
                                  WHERE      `ctl`.`etl_insrt_dt` >= `ref`.`last_ref_change`
                       )
                       ,
                       bdm_control_cnt AS
                       (
                                SELECT   `bdm_control`.`iptv_pipeline`,
                                         `bdm_control`.`job_execution_range`,
                                         `bdm_control`.`exec_dt`,
                                         count(DISTINCT `bdm_control`.`job_name`) AS `total_success_cnt`
                                FROM     bdm_control
                                GROUP BY `bdm_control`.`iptv_pipeline`,
                                         `bdm_control`.`job_execution_range`,
                                         `bdm_control`.`exec_dt`
                       )
                       ,
                       completed_cycles AS
                       (
                                  SELECT     `a`.`job_name` ,
                                             `a`.`iptv_job_name`,
                                             max(`a`.`iptv_end_curr_max_value`) AS `maxval_of_last_fin_cycle`,
                                             max(`a`.`last_run_date`)           AS `rundt_of_last_fin_cycle`
                                  FROM       bdm_control_cnt b
                                  INNER JOIN bdm_control a
                                  ON         `a`.`iptv_pipeline` = `b`.`iptv_pipeline`
                                  AND        `a`.`job_execution_range` = `b`.`job_execution_range`
                                  AND        `a`.`exec_dt` = `b`.`exec_dt`
                                  WHERE      `b`.`total_success_cnt` >= `a`.`pipeline_cnt`
                                  AND        `a`.`job_name` IN
                                             (
                                                    SELECT `ref_fltr`.`job_name`
                                                    FROM   ref_fltr )
                                  GROUP BY   `a`.`job_name`,
                                             `a`.`iptv_job_name`
                       )
                       ,
                       glgn_ctl AS
                       (
                                  SELECT     `rcf`.`job_name`                                    AS `glgn_job_name`,
                                             cast(`rcf`.`delta_col_prev_max_value` AS timestamp) AS `etl_min_value`,
                                             cast(`rcf`.`delta_col_curr_max_value` AS timestamp) AS `etl_max_value`
                                  FROM       ref_fltr flt
                                  INNER JOIN `cfw_hdp`.`glgn_job_execution` `rcf`
                                  ON         `rcf`.`job_name` = `flt`.`iptv_job_name`
                                  WHERE      `rcf`.`execution_status` = 'SUCCEEDED'
                       )
                       ,
                       cfw AS
                       (
                                       SELECT          `etl`.`glgn_job_name`,
                                                       min(to_date(`etl`.`etl_min_value`)) AS `min_value`,
                                                       max(to_date(`etl`.`etl_max_value`)) AS `max_value`,
                                                       min(`etl`.`etl_min_value`)          AS `min_value_ts`,
                                                       max(`etl`.`etl_max_value`)          AS `max_value_ts`
                                       FROM            glgn_ctl etl
                                       LEFT OUTER JOIN completed_cycles ctl
                                       ON              `etl`.`glgn_job_name` = `ctl`.`iptv_job_name`
                                       WHERE           `etl`.`etl_min_value` >= `ctl`.`maxval_of_last_fin_cycle`
                                       OR              `etl`.`etl_max_value` >= CURRENT_DATE()
                                       GROUP BY        `etl`.`glgn_job_name`
                       )
                       ,
                       cfw_nrm AS
                       (
                              SELECT `cfw`.`glgn_job_name`,
                                     `cfw`.`min_value_ts`,
                                     `cfw`.`max_value_ts`,
                                     date_add(`cfw`.`min_value`,`pe`.`i`)                                                              AS `date_value`
                              FROM   cfw lateral VIEW posexplode(split(space(datediff(`cfw`.`max_value`,`cfw`.`min_value`)),' ')) `pe` AS `i`,
                                     `x`
                       )
                       ,
                       raw_filtered AS
                       (
                                SELECT   `r`.`header`.`uuid`,
                                         `r`.`header`.`timestamp`,
                                         `r`.`received_date`,
                                         to_date(from_unixtime(cast(`r`.`header`.`timestamp`/1000 AS bigint))) AS `raw_event_date`,
                                         `r`.`hdp_create_ts`,
                                         `r`.`hdp_update_ts`,
                                         `cfw_nrm`.`min_value_ts`,
                                         `cfw_nrm`.`max_value_ts`,
                                         row_number() OVER (partition BY `r`.`header`.`uuid` ORDER BY `r`.`header`.`timestamp` DESC) AS `rnk`
                                FROM     `iptv`.`error_raw` `r`
                                JOIN     cfw_nrm
                                ON       `r`.`received_date` = `cfw_nrm`.`date_value`
                                WHERE    `r`.`hdp_create_ts` > `cfw_nrm`.`min_value_ts`
                                AND      `r`.`hdp_create_ts` <= `cfw_nrm`.`max_value_ts`
                       )SELECT     `f`.`header`.`timestamp`,
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
						 `f`.`device`.`receiverid`,
                         `f`.`device`.`deviceid`,
                         `f`.`device`.`devicesourceid`,
                         `f`.`device`.`account`,
                         `f`.`device`.`accountsourceid`,
                         `f`.`device`.`billingaccountid`,
                         `f`.`device`.`macaddress`,
                         `f`.`device`.`ecmmacaddress`,
                         `f`.`device`.`firmwareversion`,
                         `f`.`device`.`devicetype`,
                         `f`.`device`.`make`,
                         `f`.`device`.`model`,
                         `f`.`device`.`partner`,
                         `f`.`device`.`ipaddress`,
                         `f`.`device`.`utcoffset`,
                         `f`.`device`.`postalcode`,
                         `f`.`eventtimestamp`,
                         `f`.`errorcode`,
                         `f`.`errordescription`,
                         `f`.`isvisible`,
                         `f`.`isstartup`,
                         `f`.`hdp_file_name`,
                         `f`.`hdp_create_ts`,
                         `f`.`hdp_update_ts`,
                         `f`.`event_date`,
                         `r`.`received_date`,
                         `r`.`raw_event_date`,
                         `r`.`hdp_create_ts` AS `raw_hdp_create_ts`,
                         `r`.`hdp_update_ts` AS `raw_hdp_update_ts`,
                         `r`.`min_value_ts`  AS `cfw_min_value_ts`,
                         `r`.`max_value_ts`  AS `cfw_max_value_ts`
              FROM       raw_filtered r
              INNER JOIN `iptv`.`error_fct` `f`
              ON         `f`.`event_date` = `r`.`raw_event_date`
              AND        `f`.`header`.`uuid` = `r`.`uuid`
              WHERE      `r`.`rnk` = 1
              AND        to_date(`f`.`event_date`) > to_date(date_sub(CURRENT_DATE(),9));
