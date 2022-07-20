CREATE OR REPLACE VIEW `iptv.reconnect_raw_tmly_vw` 
AS SELECT `reconnect_raw`.`header`.`UUID` AS `UUID`FROM `IPTV`.`reconnect_raw`
WHERE `reconnect_raw`.`received_date` = DATE_SUB(CURRENT_DATE(),1)
GROUP BY `reconnect_raw`.`header`.`UUID`;
