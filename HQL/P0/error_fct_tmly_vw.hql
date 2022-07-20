CREATE OR REPLACE VIEW `iptv.error_fct_tmly_vw` AS 
SELECT `error_fct`.`header`.`UUID` AS `UUID`FROM `iptv`.`error_fct`
WHERE ( `error_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),1),'yyyy-MM-dd'),1,10) or
`error_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),2),'yyyy-MM-dd'),1,10)  or
`error_fct`.`event_date` = substring(date_format(CURRENT_DATE(),'yyyy-MM-dd'),1,10) )
GROUP BY `error_fct`.`header`.`UUID`;
