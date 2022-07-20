CREATE VIEW `iptv.ip_playback_raw_cnstcy_vw` AS 
SELECT `ip_playback_raw`.`header`.`UUID` AS `UUID` ,
row_number() over (PARTITION BY `ip_playback_raw`.`header`.`uuid` order by `ip_playback_raw`.`header`.`TIMESTAMP` desc) AS `rnk`
FROM `IPTV`.`IP_PLAYBACK_RAW`WHERE `ip_playback_raw`.`received_date`  = DATE_SUB(CURRENT_DATE(),1) AND UPPER(`ip_playback_raw`.`eventtype`) = 'END';
