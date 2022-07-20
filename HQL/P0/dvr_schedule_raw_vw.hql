CREATE OR REPLACE VIEW `iptv`.`dvr_schedule_raw_vw` AS 
with CFW as (
select date_add(current_date(),-8) as `min_value` , date_add(current_date(),-1) as `max_value`
),
CFW_NRM as (
select
date_add(`cfw`.`min_value`,`pe`.`i`)  as `date_value`  from CFW
lateral view
posexplode(split(space(datediff(cfw.max_value,cfw.min_value)),' ')) `pe` as `i`,`x`
)
SELECT
`compact`.`timestamp` ,
`compact`.`uuid` ,
`compact`.`hostname` ,
`compact`.`traceid` ,
`compact`.`spanid` ,
`compact`.`parentspanid` ,
`compact`.`spanname` ,
`compact`.`appname` ,
`compact`.`starttime` ,
`compact`.`spanduration` ,
`compact`.`spansuccess` ,
`compact`.`partner` ,
`compact`.`eventtimestamp` ,
`compact`.`description` ,
`compact`.`receiverid` ,
`compact`.`deviceid` ,
`compact`.`devicesourceid` ,
`compact`.`account` ,
`compact`.`accountsourceid` ,
`compact`.`billingaccountid` ,
`compact`.`macaddress` ,
`compact`.`ecmmacaddress` ,
`compact`.`firmwareversion` ,
`compact`.`devicetype` ,
`compact`.`make` ,
`compact`.`model` ,
`compact`.`device_partner`,
`compact`.`ipaddress` ,
`compact`.`utcoffset` ,
`compact`.`postalcode` ,
`compact`.`dma` ,
`compact`.`fullschedule` ,
`compact`.`hdp_file_name` ,
`compact`.`hdp_create_ts` ,
`compact`.`hdp_update_ts`,
`compact`.`received_date`
FROM (
SELECT
`r`.`header`.`TIMESTAMP` ,
`r`.`header`.`UUID` ,
`r`.`header`.`HOSTNAME` ,
`r`.`header`.`MONEY`.`TRACEID` ,
`r`.`header`.`MONEY`.`SPANID` ,
`r`.`header`.`MONEY`.`PARENTSPANID` ,
`r`.`header`.`MONEY`.`SPANNAME` ,
`r`.`header`.`MONEY`.`APPNAME` ,
`r`.`header`.`MONEY`.`STARTTIME` ,
`r`.`header`.`MONEY`.`SPANDURATION` ,
`r`.`header`.`MONEY`.`SPANSUCCESS` ,
`r`.`partner` ,
`r`.`eventtimestamp` ,
`r`.`description` ,
`r`.`device`.`RECEIVERID` ,
`r`.`device`.`DEVICEID` ,
`r`.`device`.`DEVICESOURCEID` ,
`r`.`device`.`ACCOUNT` ,
`r`.`device`.`ACCOUNTSOURCEID` ,
`r`.`device`.`BILLINGACCOUNTID` ,
`r`.`device`.`MACADDRESS` ,
`r`.`device`.`ECMMACADDRESS` ,
`r`.`device`.`FIRMWAREVERSION` ,
`r`.`device`.`DEVICETYPE` ,
`r`.`device`.`MAKE` ,
`r`.`device`.`MODEL` ,
`r`.`device`.`PARTNER` AS `DEVICE_PARTNER`,
`r`.`device`.`IPADDRESS` ,
`r`.`device`.`UTCOFFSET` ,
`r`.`device`.`POSTALCODE` ,
`r`.`device`.`DMA` ,
`r`.`fullschedule` ,
`r`.`hdp_file_name` ,
`r`.`hdp_create_ts` ,
`r`.`hdp_update_ts`,
`r`.`received_date`,
ROW_NUMBER() OVER (PARTITION BY `r`.`header`.`UUID` ORDER BY `r`.`header`.`TIMESTAMP` DESC) AS `RNK`
FROM  `IPTV`.`DVR_SCHEDULE_RAW` `R`
JOIN CFW_NRM ON `r`.`received_date` = `cfw_nrm`.`date_value`
WHERE to_date(from_unixtime(cast(`r`.`header`.`timestamp`/1000 AS bigint))) = date_add(current_date,-8)
) `COMPACT`
WHERE `compact`.`rnk` = 1;
