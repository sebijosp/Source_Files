CREATE OR REPLACE VIEW `iptv.error_raw_tmly_vw` AS 
SELECT `error_raw`.`header`.`UUID` AS `UUID` FROM `IPTV`.`error_raw`
WHERE `error_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1)
GROUP BY `error_raw`.`header`.`UUID`;
