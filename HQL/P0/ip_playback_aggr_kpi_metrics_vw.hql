CREATE OR REPLACE VIEW `iptv`.`ip_playback_aggr_kpi_metrics_vw` AS WITH cfw AS
  (SELECT CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()-854000))AS DATE) AS `min_value`,
          CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()-85400))AS DATE) AS `max_value`),
                       cfw_nrm AS
  (SELECT `cfw`.`max_value`,
          `cfw`.`min_value`,
          date_add(`cfw`.`min_value`, `pe`.`i`) AS `date_value`
   FROM cfw LATERAL VIEW posexplode(split(space(DATEDIFF(`cfw`.`max_value`, `cfw`.`min_value`)), ' ')) `pe` AS `i`,
                         `x`),
                       IP_PLAYBACK_FILTERED AS
  (SELECT `ip_playback_raw`.`received_date`,
          sum(`ip_playback_raw`.`sessionduration`)/COUNT(DISTINCT `ip_playback_raw`.`device`.`accountsourceid`) AS `viewership_per_account`,
          count(DISTINCT `ip_playback_raw`.`device`.`devicesourceid`) AS `device_cnt`,
          count(DISTINCT `ip_playback_raw`.`session`) AS `sessions_cnt`
   FROM `iptv`.`ip_playback_raw`
   JOIN cfw_nrm ON `ip_playback_raw`.`received_date` = `cfw_nrm`.`date_value`
   WHERE `ip_playback_raw`.`eventtype` = 'End'
   GROUP BY `ip_playback_raw`.`received_date`),
                       IP_PLAYBACK_AVG AS
  (SELECT max(`ip_playback_filtered`.`received_date`) AS `avg_received_date`,
          avg(`ip_playback_filtered`.`viewership_per_account`) AS `avg_viewership_per_account`,
          avg(`ip_playback_filtered`.`device_cnt`) AS `avg_device_cnt`,
          avg(`ip_playback_filtered`.`sessions_cnt`) AS `avg_sessions_cnt`
   FROM IP_PLAYBACK_FILTERED)
SELECT `ip_playback_filtered`.`received_date`,
       `ip_playback_filtered`.`viewership_per_account`,
       `ip_playback_filtered`.`device_cnt`,
       `ip_playback_filtered`.`sessions_cnt`,
       `ip_playback_avg`.`avg_viewership_per_account`,
       `ip_playback_avg`.`avg_device_cnt`,
       `ip_playback_avg`.`avg_sessions_cnt`
FROM IP_PLAYBACK_FILTERED
JOIN IP_PLAYBACK_AVG ON `ip_playback_filtered`.`received_date` = `ip_playback_avg`.`avg_received_date`;
