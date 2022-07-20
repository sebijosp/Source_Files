CREATE  OR REPLACE VIEW `iptv.tune_raw_tmly_vw` AS 
SELECT `tune_raw`.`header`.`UUID` AS `UUID`FROM `IPTV`.`tune_raw`
WHERE `tune_raw`.`received_date` =  DATE_SUB(CURRENT_DATE(),1)
and `tune_raw`.`tunestatus` NOT IN ( 'NOT_SUCCESS_OR_FAILURE'  )
GROUP BY `tune_raw`.`header`.`UUID`;
