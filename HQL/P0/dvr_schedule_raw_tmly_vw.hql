CREATE OR REPLACE VIEW `iptv.dvr_schedule_raw_tmly_vw` AS
 SELECT `dvr_schedule_raw`.`header`.`UUID` AS `UUID` FROM `IPTV`.`dvr_schedule_raw`
WHERE `dvr_schedule_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1)
GROUP BY `dvr_schedule_raw`.`header`.`UUID`;
