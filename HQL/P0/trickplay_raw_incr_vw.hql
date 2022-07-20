CREATE OR REPLACE VIEW `iptv`.`trickplay_raw_incr_vw` AS WITH ref_tbl                 AS
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
                              AND    `ref_tbl`.`iptv_source_table` = 'IPTV.TRICKPLAY_RAW'
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
                                                    FROM   ref_fltr)
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
                       )SELECT `compact`.`timestamp`,
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
					   `compact`.`receiverid`,
                       `compact`.`deviceid`,
                       `compact`.`devicesourceid`,
                       `compact`.`account`,
                       `compact`.`accountsourceid`,
                       `compact`.`billingaccountid`,
                       `compact`.`macaddress`,
                       `compact`.`ecmmacaddress`,
                       `compact`.`firmwareversion`,
                       `compact`.`devicetype`,
                       `compact`.`make`,
                       `compact`.`model`,
                       `compact`.`partner`,
                       `compact`.`ipaddress`,
                       `compact`.`utcoffset`,
                       `compact`.`postalcode`,
                       `compact`.`eventtimestamp`,
                       `compact`.`trickplayname`,
                       `compact`.`playbackrate`,
                       `compact`.`assetclass`,
                       `compact`.`assetposition`,
                       `compact`.`hdp_file_name`,
                       `compact`.`hdp_create_ts`,
                       `compact`.`hdp_update_ts`,
                       `compact`.`received_date`
                FROM   (
                                SELECT   `r`.`header`.`timestamp`,
                                         `r`.`header`.`uuid`,
                                         `r`.`header`.`hostname`,
                                         `r`.`header`.`money`.`traceid`,
                                         `r`.`header`.`money`.`spanid`,
                                         `r`.`header`.`money`.`parentspanid`,
                                         `r`.`header`.`money`.`spanname`,
                                         `r`.`header`.`money`.`appname`,
                                         `r`.`header`.`money`.`starttime`,
                                         `r`.`header`.`money`.`spanduration`,
                                         `r`.`header`.`money`.`spansuccess`,
										 `r`.`header`.`money`.`notes`,
                                         `r`.`device`.`receiverid`,
                                         `r`.`device`.`deviceid`,
                                         `r`.`device`.`devicesourceid`,
                                         `r`.`device`.`account`,
                                         `r`.`device`.`accountsourceid`,
                                         `r`.`device`.`billingaccountid`,
                                         `r`.`device`.`macaddress`,
                                         `r`.`device`.`ecmmacaddress`,
                                         `r`.`device`.`firmwareversion`,
                                         `r`.`device`.`devicetype`,
                                         `r`.`device`.`make`,
                                         `r`.`device`.`model`,
                                         `r`.`device`.`partner`,
                                         `r`.`device`.`ipaddress`,
                                         `r`.`device`.`utcoffset`,
                                         `r`.`device`.`postalcode`,
                                         `r`.`eventtimestamp`,
                                         `r`.`trickplayname`,
                                         `r`.`playbackrate`,
                                         `r`.`assetclass`,
                                         `r`.`assetposition`,
                                         `r`.`hdp_file_name`,
                                         `r`.`hdp_create_ts`,
                                         `r`.`hdp_update_ts`,
                                         `r`.`received_date`,
                                         row_number() OVER (partition BY `r`.`header`.`uuid` ORDER BY `r`.`header`.`timestamp` DESC) AS `rnk`
                                FROM     `iptv`.`trickplay_raw` `r`
                                JOIN     cfw_nrm
                                ON       `r`.`received_date` = `cfw_nrm`.`date_value`
                                WHERE    `r`.`hdp_create_ts` > `cfw_nrm`.`min_value_ts`
                                AND      `r`.`hdp_create_ts` <= `cfw_nrm`.`max_value_ts`) `compact`
                WHERE  `compact`.`rnk` = 1;
