CREATE OR REPLACE VIEW `iptv.error_raw_cnstcy_vw` AS SELECT `error_raw`.`header`.`UUID` AS `UUID` ,
row_number() over (PARTITION BY `error_raw`.`header`.`uuid` order by `error_raw`.`header`.`TIMESTAMP` desc) AS `rnk`FROM `IPTV`.`ERROR_RAW`
WHERE `error_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1);
