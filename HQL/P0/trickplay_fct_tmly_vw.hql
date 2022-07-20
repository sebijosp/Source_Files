CREATE OR REPLACE VIEW `iptv.trickplay_fct_tmly_vw` AS 
SELECT `trickplay_fct`.`header`.`UUID` AS `UUID`FROM `iptv`.`trickplay_fct`
WHERE ( `trickplay_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),1),'yyyy-MM-dd'),1,10) or
`trickplay_fct`.`event_date` =  substring(date_format(DATE_SUB(CURRENT_DATE(),2),'yyyy-MM-dd'),1,10)  or 
`trickplay_fct`.`event_date` =  substring(date_format(CURRENT_DATE(),'yyyy-MM-dd'),1,10) 
)
GROUP BY `trickplay_fct`.`header`.`UUID`;
