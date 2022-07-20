CREATE OR REPLACE VIEW `iptv.dvr_schedule_fct_tmly_vw` AS 
SELECT `dvr_schedule_fct`.`header`.`UUID` AS `UUID`FROM `iptv`.`dvr_schedule_fct`
WHERE ( `dvr_schedule_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),1),'yyyy-MM-dd'),1,10) or 
`dvr_schedule_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),2),'yyyy-MM-dd'),1,10)  or 
`dvr_schedule_fct`.`event_date` = substring(date_format(CURRENT_DATE(),'yyyy-MM-dd'),1,10) )
GROUP BY `dvr_schedule_fct`.`header`.`UUID`;
