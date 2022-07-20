drop table iptv.dvr_schedules_lkp;

CREATE TABLE iptv.dvr_schedules_lkp
(
recordingid string,
accountid string,
deviceId string,
title string,
startTime string,
duration string,
listingid string,
stationid string, 
seriesid string,
entityid string,
programid string,
channelid string,
channelnumber  string
)
COMMENT 'temp table for dvr_schedules_analysis'
STORED AS ORC
