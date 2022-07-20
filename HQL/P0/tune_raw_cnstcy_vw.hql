CREATE OR REPLACE VIEW `iptv.tune_raw_cnstcy_vw` AS SELECT `tune_raw`.`header`.`UUID` AS `UUID` , 
row_number() over (PARTITION BY `tune_raw`.`header`.`uuid` order by `tune_raw`.`header`.`TIMESTAMP` desc) AS `rnk` 
FROM `IPTV`.`TUNE_RAW` WHERE `tune_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1) and
 lower(`tune_raw`.`tunestatus` ) not in ( 'not_success_or_failure' );
