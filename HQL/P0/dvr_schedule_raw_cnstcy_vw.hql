CREATE OR REPLACE VIEW `iptv.dvr_schedule_raw_cnstcy_vw` AS 
SELECT `dvr_schedule_raw`.`header`.`UUID` AS `UUID` ,
row_number() over (PARTITION BY `dvr_schedule_raw`.`header`.`uuid` order by `dvr_schedule_raw`.`header`.`TIMESTAMP` desc) AS `rnk`
FROM `IPTV`.`DVR_SCHEDULE_RAW`WHERE `dvr_schedule_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1);
