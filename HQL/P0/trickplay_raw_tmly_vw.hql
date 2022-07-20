CREATE OR REPLACE VIEW `iptv.trickplay_raw_tmly_vw` AS 
SELECT `trickplay_raw`.`header`.`UUID` AS `UUID` 
FROM `IPTV`.`trickplay_raw`WHERE `trickplay_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1)
GROUP BY `trickplay_raw`.`header`.`UUID`;
