!echo "dropping the table cdrdm.gmpc";

drop table if exists cdrdm.gmpc;

!echo "creating the table cdrdm.gmpc";

CREATE TABLE IF NOT EXISTS `cdrdm.gmpc`(
  `file_name` string,
  `record_start` bigint,
  `record_end` bigint,
  `record_type` string,
  `family_name` string,
  `version_id` int,
  `file_time` string,
  `file_id` string,
  `switch_name` string,
  `num_records` string,
  `clientid` string,
  `clientno` string,
  `clientrequestor` string,
  `clientservicetype` string,
  `clienttype` string,
  `coordinatesystem` string,
  `datum` string,
  `dialledbyms` string,
  `locationtype` string,
  `network` string,
  `numbersgsn` string,
  `numbervlr` string,
  `obtainedaccuracy` string,
  `privacyoverride` string,
  `pushid` string,
  `pushurl` string,
  `requestedpositiontime` string,
  `setid` string,
  `subclientno` string,
  `trafficcase` string,
  `usedlocationmethod` string,
  `callduration` string,
  `esrd` string,
  `numbermme` string,
  `numberamf` string,
  `requestedaccuracy` string,
  `requestedhoraccuracymeters` string,
  `responsetime` string,
  `maxlocationage` string,
  `targetms` string,
  `numbermsc` string,
  `positiontime` string,
  `levelofconfidence` string,
  `obtainedhoraccuracy` string,
  `requestbearer` string,
  `latitude` string,
  `longitude` string,
  `errorcode` string,
  `errorcode1` string,
  `qos` string,
  `singlepositions` string,
  `positiondatarecord` string,
  `imei` string,
  `imsi` string,
  `positionresult` string,
  `iscached` string)
PARTITIONED BY (
  `file_date` string)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");

