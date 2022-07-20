alter table cdrdm.fact_gmpc_cdr rename to cdrdm.fact_gmpc_cdr_postmps;
msck repair table cdrdm.fact_gmpc_cdr_postmps;

CREATE TABLE `cdrdm.fact_gmpc_cdr`(
  `trafficcase` string,
  `locationtype` string,
  `clientid` varchar(50),
  `clientno` varchar(20),
  `pushurl` varchar(350),
  `pushid` varchar(100),
  `errorcode1` smallint,
  `clienttype` string,
  `privacyoverride` smallint,
  `coordinatesystem` string,
  `datum` string,
  `requestedaccuracy` string,
  `requestedaccuracymeters` int,
  `responsetime` string,
  `requestedpositiontime` string,
  `subclientno` varchar(20),
  `clientrequestor` varchar(20),
  `clientservicetype` smallint,
  `targetms` varchar(20),
  `numbermsc` varchar(20),
  `numbersgsn` varchar(20),
  `positiontime` string,
  `levelofconfidence` smallint,
  `obtainedaccuracy` int,
  `errorcode2` smallint,
  `dialledbyms` varchar(20),
  `numbervlr` varchar(20),
  `requestbearer` string,
  `setid` varchar(100),
  `latitude_v` varchar(100),
  `longitude_v` varchar(100),
  `latitude` decimal(10,5),
  `longitude` decimal(10,5),
  `province_id` smallint,
  `network` string,
  `usedlocationmethod` string,
  `esrd` varchar(25),
  `numbermme` varchar(128),
  `callduration` int,
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
  `insert_timestamp` string,
  `cfnumber_orig` string,
  `positiondatarecord` string,
  `cdr_type` string,
  `imei` string,
  `imsi` string,
  `positionresult` string,
  `iscached` string)
PARTITIONED BY (
  `position_date` string)
STORED AS ORC
TBLPROPERTIES (
  'orc.row.index.stride'='700000',
  'orc.stripe.size'='536870912',
  'orc.compress'='SNAPPY'
);

ALTER TABLE cdrdm.fact_gmpc_cdr SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

INSERT INTO TABLE cdrdm.FACT_GMPC_CDR PARTITION (position_date)
select * from cdrdm.fact_gmpc_cdr_postmps where position_Date="--" and not (file_name like 'gmpc_ms1_Billing1394_2016%' or file_name like 'gmpc_ml02_Billing1372_2016%');
msck repair table cdrdm.FACT_GMPC_CDR;

INSERT INTO TABLE cdrdm.FACT_GMPC_CDR PARTITION (position_date)
select * from cdrdm.fact_gmpc_cdr_postmps where position_Date <> "--" and not (file_name like 'gmpc_ms1_Billing1394_2016%' or file_name like 'gmpc_ml02_Billing1372_2016%');
msck repair table cdrdm.FACT_GMPC_CDR;
