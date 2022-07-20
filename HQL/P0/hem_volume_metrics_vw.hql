CREATE OR REPLACE VIEW `hem`.`hem_volume_metrics_vw` AS WITH cfw AS
  (SELECT date_sub(current_date,10) AS `min_value`,
   date_sub(current_date,1) AS `max_value`
  ),
  cfw_nrm AS
  (SELECT `cfw`.`max_value`,
    `cfw`.`min_value`,
    date_add(`cfw`.`min_value`,`pe`.`i`)                                                                 AS `date_value`
  FROM cfw lateral VIEW posexplode(split(space(DATEDIFF(cfw.max_value,cfw.min_value)),' ')) `pe` AS `i`,
    `x`
  ),
  FEED_FILTERED AS
  (SELECT 'IPDR_S06' AS `feed`,
   `cfw_nrm`.`date_value` as `received_date`,
    case when `r`.`row_count` is null then 0 else `r`.`row_count` end as `row_count`
  from cfw_nrm
  left join (
  select to_date(`ipdr_s06`.`processed_date`) as `received_date`,
  COUNT(`ipdr_s06`.`processed_date`) AS `row_count`
  from `ipdr`.`ipdr_s06`
  where `ipdr_s06`.`processed_date` between  date_sub(current_date, 10) and date_sub(current_date, 1)
  group by `ipdr_s06`.`processed_date`
  ) `r`
  ON `cfw_nrm`.`date_value` = `r`.`received_date`
  union
  SELECT 'IPDR_S10' AS `feed`,
   `cfw_nrm`.`date_value` as `received_date`,
  case when `r`.`row_count` is null then 0 else `r`.`row_count` end as `row_count`
  from cfw_nrm
  left join (
  select to_date(`ipdr_s10`.`processed_ts`) as `received_date`,
    COUNT(to_date(`ipdr_s10`.`processed_ts`)) AS `row_count`
  from `ipdr`.`ipdr_s10`
  where to_date(`ipdr_s10`.`processed_ts`) between  date_sub(current_date, 10) and date_sub(current_date, 1)
  GROUP BY to_date(`ipdr_s10`.`processed_ts`)) `r`
  ON `cfw_nrm`.`date_value` = `r`.`received_date`
    union
  SELECT 'IPDR_S14' AS `feed`,
   `cfw_nrm`.`date_value` as `received_date`,
    case when `r`.`row_count` is null then 0 else `r`.`row_count` end as `row_count`
  from cfw_nrm
  left join (select to_date(`ipdr_s14`.`processed_ts`) as `received_date`,
    COUNT(to_date(`ipdr_s14`.`processed_ts`)) AS `row_count`
  from `ipdr`.`ipdr_s14`
  where to_date(`ipdr_s14`.`processed_ts`) between  date_sub(current_date, 10) and date_sub(current_date, 1)
  GROUP BY to_date(`ipdr_s14`.`processed_ts`)) `r`
  ON `cfw_nrm`.`date_value` = `r`.`received_date`
    union
  SELECT 'IPDR_S15' AS `feed`,
   `cfw_nrm`.`date_value` as `received_date`,
    case when `r`.`row_count` is null then 0 else `r`.`row_count` end as `row_count`
  from cfw_nrm
  left join (select
  to_date(`ipdr_s15`.`processed_ts`) as `received_date`,
    COUNT(to_date(`ipdr_s15`.`processed_ts`)) AS `row_count`
  from `ipdr`.`ipdr_s15`
  where to_date(`ipdr_s15`.`processed_ts`) between  date_sub(current_date, 10) and date_sub(current_date, 1)
  GROUP BY to_date(`ipdr_s15`.`processed_ts`)
  ) `r`
  ON `cfw_nrm`.`date_value` = `r`.`received_date`
    union
  SELECT 'MODEM_ACCESSIBILITY_DAILY' AS `feed`,
   `cfw_nrm`.`date_value` as `received_date`,
    case when `r`.`row_count` is null then 0 else `r`.`row_count` end as `row_count`
  from cfw_nrm
  left join ( select
   to_date(`modem_accessibility_daily`.`event_date`) as `received_date`,
   COUNT(to_date(`modem_accessibility_daily`.`event_date`)) AS `row_count`
  from `hem`.`MODEM_ACCESSIBILITY_DAILY`
  where  to_date(`modem_accessibility_daily`.`event_date`) between  date_sub(current_date, 10) and date_sub(current_date, 1)
  GROUP BY to_date(`modem_accessibility_daily`.`event_date`)
  )`r`
  ON `cfw_nrm`.`date_value` = `r`.`received_date`
  ),
  FEED_AVG AS
  (SELECT `feed_filtered`.`feed`,
    MAX(`feed_filtered`.`received_date`) AS `received_date`,
    AVG(`feed_filtered`.`row_count`) AS `row_count`
  FROM feed_FILTERED
  GROUP BY `feed_filtered`.`feed`
  ) ,
  FEED_FINAL AS
  (SELECT `feed_filtered`.`feed`,
    `feed_filtered`.`received_date`,
    `feed_filtered`.`row_count` AS `current_count`,
    `feed_avg`.`row_count` AS `average_count`
  FROM feed_FILTERED
  INNER JOIN FEED_AVG
  ON `feed_filtered`.`feed`=`feed_avg`.`feed`
  and `feed_filtered`.`received_date` = `feed_avg`.`received_date`
  ORDER BY `feed_filtered`.`feed`
 ),
 FEED_COL AS
 (select
      `feed_final`.`received_date`,
      COLLECT_LIST(`feed_final`.`feed`) `D_FEED`,
      COLLECT_LIST(`feed_final`.`current_count`) `D_CURRENT_CNT`,
      COLLECT_LIST(`feed_final`.`average_count`) `D_AVG_CNT`
      from FEED_FINAL
      GROUP BY `feed_final`.`received_date`
  )
  select
      `feed_col`.`received_date`,
      `feed_col`.`d_feed`[0] AS `s06_feed`,
      `feed_col`.`d_current_cnt`[0] as `s06_current_count`,
      `feed_col`.`d_avg_cnt`[0] as `s06_average_count`,
      `feed_col`.`d_feed`[1] AS `s10_feed`,
      `feed_col`.`d_current_cnt`[1] as `s10_current_count`,
      `feed_col`.`d_avg_cnt`[1] as `s10_average_count`,
      `feed_col`.`d_feed`[2] AS `s14_feed`,
      `feed_col`.`d_current_cnt`[2] as `s14_current_count`,
      `feed_col`.`d_avg_cnt`[2] as `s14_average_count`,
      `feed_col`.`d_feed`[3] AS `s15_feed`,
      `feed_col`.`d_current_cnt`[3] as `s15_current_count`,
      `feed_col`.`d_avg_cnt`[3] as `s15_average_count`,
      `feed_col`.`d_feed`[4] AS `modem_accessibility_daily_feed`,
      `feed_col`.`d_current_cnt`[4] as `modem_accessibility_daily_cnt`,
      `feed_col`.`d_avg_cnt`[4] as `modem_accessibility_daily_avg`
  from FEED_COL;
