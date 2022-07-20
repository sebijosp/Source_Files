CREATE OR REPLACE VIEW `iptv`.`iptv_volume_metrics_vw` AS
WITH cfw AS
  (SELECT CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()-864000))AS DATE) AS `min_value`,
    CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE) AS `max_value`
  ),
  cfw_nrm AS
  (SELECT `cfw`.`max_value`,
    `cfw`.`min_value`,
    date_add(`cfw`.`min_value`,`pe`.`i`)                                                                 AS `date_value`
  FROM cfw lateral VIEW posexplode(split(space(DATEDIFF(`cfw`.`max_value`,`cfw`.`min_value`)),' ')) `pe` AS `i`,
    `x`
  ),
  FEED_DUMMY AS (
select
  CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE) as `received_date`,
  'IP_PLAYBACK' as `IP_PLAYBACK_FEED`,
  'DVR_SCHEDULE' as `DVR_SCHEDULE_FEED`,
  'ERROR' as `ERROR_FEED`,
  'TRICKPLAY' as `TRICKPLAY_FEED`,
  'RECONNECT' as `RECONNECT_FEED`,
  'TUNE' as `TUNE_FEED`,
  'APP_EVENTS' as `APP_EVENTS_FEED`,
  'GUIDE_START_ERROR' as `GUIDE_START_ERROR_FEED`,
  'PAGEVIEW_EVENT' as `PAGEVIEW_EVENT_FEED`,
  'SEARCH_MESSAGE' as `SEARCH_MESSAGE_FEED`
),
  IP_PLAYBACK_FILTERED AS
  (SELECT 'IP_PLAYBACK_RAW' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`ip_playback_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  IP_PLAYBACK_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`ip_playback_filtered`.`row_count`) AS `row_count`
  FROM IP_PLAYBACK_FILTERED
  GROUP BY feed
  ),
  IP_PLAYBACK_FINAL AS
  (SELECT `ip_playback_filtered`.`feed`,
    `ip_playback_filtered`.`received_date`,
    `ip_playback_filtered`.`row_count` AS `current_count`,
    `ip_playback_avg`.`row_count` AS `average_count`
  FROM IP_PLAYBACK_FILTERED
  INNER JOIN IP_PLAYBACK_AVG
  ON `IP_PLAYBACK_FILTERED`.`feed`=`ip_playback_avg`.`feed`
  AND `ip_playback_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  DVR_SCHEDULE_FILTERED AS
  (SELECT 'DVR_SCHEDULE' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`DVR_SCHEDULE_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  DVR_SCHEDULE_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`dvr_schedule_filtered`.`row_count`) AS `row_count`
  FROM DVR_SCHEDULE_FILTERED
  GROUP BY feed
  ),
  DVR_SCHEDULE_FINAL AS
  (SELECT `dvr_schedule_filtered`.`feed`,
    `dvr_schedule_filtered`.`received_date`,
    `dvr_schedule_filtered`.`row_count` AS `current_count`,
    `dvr_schedule_avg`.`row_count` AS `average_count`
  FROM DVR_SCHEDULE_FILTERED
  INNER JOIN DVR_SCHEDULE_AVG
  ON `DVR_SCHEDULE_FILTERED`.`feed`=`dvr_schedule_avg`.`feed`
  AND `dvr_schedule_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  ERROR_FILTERED AS
  (SELECT 'ERROR' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`error_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  ERROR_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`error_filtered`.`row_count`) AS `row_count`
  FROM ERROR_FILTERED
  GROUP BY feed
  ),
  ERROR_FINAL AS
  (SELECT `error_filtered`.`feed`,
    `error_filtered`.`received_date`,
    `error_filtered`.`row_count` AS `current_count`,
    `error_avg`.`row_count` AS `average_count`
  FROM ERROR_FILTERED
  INNER JOIN ERROR_AVG
  ON `ERROR_FILTERED`.`feed`=`error_avg`.`feed`
  AND `error_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  RECONNECT_FILTERED AS
  (SELECT 'RECONNECT' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`RECONNECT_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  RECONNECT_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`reconnect_filtered`.`row_count`) AS `row_count`
  FROM RECONNECT_FILTERED
  GROUP BY feed
  ),
  RECONNECT_FINAL AS
  (SELECT `reconnect_filtered`.`feed`,
    `reconnect_filtered`.`received_date`,
    `reconnect_filtered`.`row_count` AS `current_count`,
    `reconnect_avg`.`row_count` AS `average_count`
  FROM RECONNECT_FILTERED
  INNER JOIN RECONNECT_AVG
  ON `RECONNECT_FILTERED`.`feed`=`reconnect_avg`.`feed`
  AND `reconnect_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  TRICKPLAY_FILTERED AS
  (SELECT 'TRICKPLAY' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`TRICKPLAY_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  TRICKPLAY_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`trickplay_filtered`.`row_count`) AS `row_count`
  FROM TRICKPLAY_FILTERED
  GROUP BY feed
  ),
  TRICKPLAY_FINAL AS
  (SELECT `trickplay_filtered`.`feed`,
    `trickplay_filtered`.`received_date`,
    `trickplay_filtered`.`row_count` AS `current_count`,
    `trickplay_avg`.`row_count` AS `average_count`
  FROM TRICKPLAY_FILTERED
  INNER JOIN TRICKPLAY_AVG
  ON `TRICKPLAY_FILTERED`.`feed`=`trickplay_avg`.`feed`
  AND `trickplay_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  TUNE_FILTERED AS
  (SELECT 'TUNE' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`TUNE_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  TUNE_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`tune_filtered`.`row_count`) AS `row_count`
  FROM TUNE_FILTERED
  GROUP BY feed
  ),
  TUNE_FINAL AS
  (SELECT `tune_filtered`.`feed`,
    `tune_filtered`.`received_date`,
    `tune_filtered`.`row_count` AS `current_count`,
    `tune_avg`.`row_count` AS `average_count`
  FROM TUNE_FILTERED
  INNER JOIN TUNE_AVG
  ON `TUNE_FILTERED`.`feed`=`tune_avg`.`feed`
  AND `tune_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  APP_EVENTS_FILTERED AS
  (SELECT 'APP_EVENTS' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`APP_EVENTS_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  APP_EVENTS_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`app_events_filtered`.`row_count`) AS `row_count`
  FROM APP_EVENTS_FILTERED
  GROUP BY feed
  ),
  APP_EVENTS_FINAL AS
  (SELECT `app_events_filtered`.`feed`,
    `app_events_filtered`.`received_date`,
    `app_events_filtered`.`row_count` AS `current_count`,
    `app_events_avg`.`row_count` AS `average_count`
  FROM APP_EVENTS_FILTERED
  INNER JOIN APP_EVENTS_AVG
  ON `APP_EVENTS_FILTERED`.`feed`=`app_events_avg`.`feed`
  AND `app_events_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  GUIDE_START_ERROR_FILTERED AS
  (SELECT 'GUIDE_START_ERROR' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`GUIDE_START_ERROR_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  GUIDE_START_ERROR_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`guide_start_error_filtered`.`row_count`) AS `row_count`
  FROM GUIDE_START_ERROR_FILTERED
  GROUP BY feed
  ),
  GUIDE_START_ERROR_FINAL AS
  (SELECT `guide_start_error_filtered`.`feed`,
    `guide_start_error_filtered`.`received_date`,
    `guide_start_error_filtered`.`row_count` AS `current_count`,
    `guide_start_error_avg`.`row_count` AS `average_count`
  FROM GUIDE_START_ERROR_FILTERED
  INNER JOIN GUIDE_START_ERROR_AVG
  ON `GUIDE_START_ERROR_FILTERED`.`feed`=`guide_start_error_avg`.`feed`
  AND `guide_start_error_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  PAGEVIEW_EVENT_FILTERED AS
  (SELECT 'PAGEVIEW_EVENT' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`PAGEVIEW_EVENT_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  PAGEVIEW_EVENT_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`pageview_event_filtered`.`row_count`) AS `row_count`
  FROM PAGEVIEW_EVENT_FILTERED
  GROUP BY feed
  ),
  PAGEVIEW_EVENT_FINAL AS
  (SELECT `pageview_event_filtered`.`feed`,
    `pageview_event_filtered`.`received_date`,
    `pageview_event_filtered`.`row_count` AS `current_count`,
    `pageview_event_avg`.`row_count` AS `average_count`
  FROM PAGEVIEW_EVENT_FILTERED
  INNER JOIN PAGEVIEW_EVENT_AVG
  ON `PAGEVIEW_EVENT_FILTERED`.`feed`=`pageview_event_avg`.`feed`
  AND `pageview_event_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  ),
  SEARCH_MESSAGE_FILTERED AS
  (SELECT 'SEARCH_MESSAGE' AS `feed`,
    `r`.`received_date`,
    COUNT(*) AS `row_count`
  FROM `iptv`.`SEARCH_MESSAGE_raw` `r`
  JOIN cfw_nrm
  ON `r`.`received_date` = `cfw_nrm`.`date_value`
  GROUP BY `r`.`received_date`
  ),
  SEARCH_MESSAGE_AVG AS
  (SELECT feed,
    NULL                            AS `received_date`,
    AVG(`search_message_filtered`.`row_count`) AS `row_count`
  FROM SEARCH_MESSAGE_FILTERED
  GROUP BY feed
  ),
  SEARCH_MESSAGE_FINAL AS
  (SELECT `search_message_filtered`.`feed`,
    `search_message_filtered`.`received_date`,
    `search_message_filtered`.`row_count` AS `current_count`,
    `search_message_avg`.`row_count` AS `average_count`
  FROM SEARCH_MESSAGE_FILTERED
  INNER JOIN SEARCH_MESSAGE_AVG
  ON `SEARCH_MESSAGE_FILTERED`.`feed`=`search_message_avg`.`feed`
  AND `search_message_filtered`.`received_date` = CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()      -86400))AS  DATE)
  )
  select
  `feed_dummy`.`received_date`,
  `feed_dummy`.`ip_playback_feed`,
  `ip_playback_final`.`current_count` as `IP_PLAYBACK_current_count`,
  `ip_playback_final`.`average_count` as `IP_PLAYBACK_average_count`,
  `feed_dummy`.`dvr_schedule_feed`,
  `dvr_schedule_final`.`current_count` as `DVR_SCHEDULE_current_count`,
  `dvr_schedule_final`.`average_count` as `DVR_SCHEDULE_average_count`,
  `feed_dummy`.`error_feed`,
  `error_final`.`current_count` as `ERROR_current_count`,
  `error_final`.`average_count` as `ERROR_average_count`,
  `feed_dummy`.`reconnect_feed`,
  `reconnect_final`.`current_count` as `RECONNECT_current_count`,
  `reconnect_final`.`average_count` as `RECONNECT_average_count`,
  `feed_dummy`.`trickplay_feed`,
  `trickplay_final`.`current_count` as `TRICKPLAY_current_count`,
  `trickplay_final`.`average_count` as `TRICKPLAY_average_count`,
  `feed_dummy`.`tune_feed`,
  `tune_final`.`current_count` as `TUNE_current_count`,
  `tune_final`.`average_count` as `TUNE_average_count`,
  `feed_dummy`.`app_events_feed`,
  `app_events_final`.`current_count` as `APP_EVENTS_current_count`,
  `app_events_final`.`average_count` as `APP_EVENTS_average_count`,
  `feed_dummy`.`guide_start_error_feed`,
  `guide_start_error_final`.`current_count` as `GUIDE_START_ERROR_current_count`,
  `guide_start_error_final`.`average_count` as `GUIDE_START_ERROR_average_count`,
  `feed_dummy`.`pageview_event_feed`,
  `pageview_event_final`.`current_count` as `PAGEVIEW_EVENT_current_count`,
  `pageview_event_final`.`average_count` as `PAGEVIEW_EVENT_average_count`,
  `feed_dummy`.`search_message_feed`,
  `search_message_final`.`current_count` as `SEARCH_MESSAGE_current_count`,
  `search_message_final`.`average_count` as `SEARCH_MESSAGE_average_count`
  from FEED_DUMMY
  left join DVR_SCHEDULE_FINAL on `feed_dummy`.`received_date` = `dvr_schedule_final`.`received_date`
  left join IP_PLAYBACK_FINAL on `FEED_DUMMY`.`received_date` = `ip_playback_final`.`received_date`
  left join ERROR_FINAL on `FEED_DUMMY`.`received_date` = `error_final`.`received_date`
  left join RECONNECT_FINAL on `FEED_DUMMY`.`received_date` = `reconnect_final`.`received_date`
  left join TRICKPLAY_FINAL on `FEED_DUMMY`.`received_date` = `trickplay_final`.`received_date`
  left join TUNE_FINAL on `FEED_DUMMY`.`received_date` = `tune_final`.`received_date`
  left join APP_EVENTS_FINAL on `FEED_DUMMY`.`received_date` = `app_events_final`.`received_date`
  left join GUIDE_START_ERROR_FINAL on `FEED_DUMMY`.`received_date` = `guide_start_error_final`.`received_date`
  left join PAGEVIEW_EVENT_FINAL on `FEED_DUMMY`.`received_date` = `pageview_event_final`.`received_date`
  left join SEARCH_MESSAGE_FINAL on `FEED_DUMMY`.`received_date` = `search_message_final`.`received_date`;