CREATE OR REPLACE VIEW `iptv.tune_fct_tmly_vw` AS 
SELECT `tune_fct`.`header`.`UUID` AS `UUID`FROM `iptv`.`tune_fct`
WHERE ( `tune_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),1),'yyyy-MM-dd'),1,10) or
`tune_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),2),'yyyy-MM-dd'),1,10)  or 
`tune_fct`.`event_date` = substring(date_format(CURRENT_DATE(),'yyyy-MM-dd'),1,10))
GROUP BY `tune_fct`.`header`.`UUID`;
