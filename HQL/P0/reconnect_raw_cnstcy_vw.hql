CREATE OR REPLACE VIEW `iptv.reconnect_raw_cnstcy_vw` AS 
SELECT `reconnect_raw`.`header`.`UUID` AS `UUID` ,
row_number() over (PARTITION BY `reconnect_raw`.`header`.`uuid` order by `reconnect_raw`.`header`.`TIMESTAMP` desc) AS `rnk`
FROM `IPTV`.`RECONNECT_RAW`WHERE `reconnect_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1);
