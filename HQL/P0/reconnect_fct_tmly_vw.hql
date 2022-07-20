CREATE OR REPLACE VIEW `iptv.reconnect_fct_tmly_vw` AS 
SELECT `reconnect_fct`.`header`.`UUID` AS `UUID`FROM 
`iptv`.`reconnect_fct`WHERE ( `reconnect_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),1),'yyyy-MM-dd'),1,10) or
`reconnect_fct`.`event_date` = substring(date_format(DATE_SUB(CURRENT_DATE(),2),'yyyy-MM-dd'),1,10)  or 
`reconnect_fct`.`event_date` = substring(date_format(CURRENT_DATE(),'yyyy-MM-dd'),1,10) )
GROUP BY `reconnect_fct`.`header`.`UUID`;
