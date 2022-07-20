CREATE OR REPLACE VIEW `iptv.ip_playback_raw_tmly_vw` AS
SELECT `ip_playback_raw`.`header`.`UUID` AS `UUID` ,  `ip_playback_raw`.asset.`assetclass`         AS `assetclass`
FROM `IPTV`.`IP_PLAYBACK_RAW`WHERE `ip_playback_raw`.`received_date` =  DATE_SUB(CURRENT_DATE(),1) AND
UPPER(`ip_playback_raw`.`eventtype`)                                                                                     = 'END'AND upper(`ip_playback_raw`.asset.`assetclass`) NOT IN ('EAS' )
GROUP BY `ip_playback_raw`.`header`.`UUID` ,  `ip_playback_raw`.asset.`assetclass`;
