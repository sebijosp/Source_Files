CREATE OR REPLACE VIEW `iptv.trickplay_raw_cnstcy_vw` AS 
SELECT `trickplay_raw`.`header`.`UUID` AS `UUID` ,
row_number() over (PARTITION BY `trickplay_raw`.`header`.`uuid` order by `trickplay_raw`.`header`.`TIMESTAMP` desc) AS `rnk`
FROM `IPTV`.`TRICKPLAY_RAW`WHERE `trickplay_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1);
