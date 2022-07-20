use default;

set hive.tez.container.size=16384;
set hive.tez.java.opts=-Xmx13108m;
set tez.am.resource.memory.mb=16384;
SET hive.execution.engine=tez;
SET tez.grouping.split-count=255;

SET WHERE_clause;

DROP TABLE IF EXISTS ext_cdrdm.pgw1215raw_non_pii_temp;

CREATE TABLE IF NOT EXISTS `ext_cdrdm.pgw1215raw_non_pii_temp`(
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
  `accesspointnameni` string,
  `apnselectionmode` string,
  `causeforrecclosing` string,
  `ccrequestnumber` string,
  `ccsrealm` string,
  `chargingcharacteristics` string,
  `chargingid` string,
  `chchselectionmode` string,
  `creditcontrolfailurereport` string,
  `creditcontrolsessionid` string,
  `cunt1` string,
  `datavolumedownlink` string,
  `datavolumeuplink` string,
  `duration` string,
  `dynamicaddressflag` string,
  `endtime` string,
  `etsiaddress` string,
  `externalchargingid` string,
  `ggsnaddress` string,
  `identifier` string,
  `imssignalingcontext` string,
  `inactivity` string,
  `listoftrafficvolumes` string,
  `listofuritimestamps` string,
  `localsequencenumber` string,
  `method` string,
  `mstimezone` string,
  `nodeid` string,
  `pcsrealm` string,
  `pdptype` string,
  `policycontrolfailurereport` string,
  `policycontrolsessionid` string,
  `ratinggroup` string,
  `rattype` string,
  `recordopeningtime` string,
  `recordsequencenumber` string,
  `recordtype` string,
  `resolution` string,
  `rulespaceid` string,
  `servedimeisv` string,
  `servedimsi` string,
  `subs_array` array<STRING>,
  `servedpdpaddress_iptextv6address` string,
  `servedpdpaddress_iptextv4address` string,
  `servedpdpaddress_ipbinv6address` string,
  `servedpdpaddress_ipbinv4address` string,
  `serviceidentifier` string,
  `servicespecificunits` string,
  `sgsnaddress` string,
  `sgsnplmnidentifier` string,
  `significance` string,
  `starttime` string,
  `uri` string,
  `uridatavolumedownlink` string,
  `uridatavolumeuplink` string,
  `uriidentifier` string,
  `usercategory` string,
  `userlocationinformation` string,
  `list_of_service_data` string,
  `servingnodetype` string,
  `p_gwplmnidentifier` string,
  `stoptime` string,
  `pdn_connection_id` string,
  `3gpp2_userlocationinformation` string,
  `served_pdp_pdn_addressext` string)
PARTITIONED BY (
  `file_date` string)
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY");


INSERT INTO ext_cdrdm.pgw1215raw_non_pii_temp partition(file_date)
SELECT a.file_name
,a.record_start
,a.record_end
,a.record_type
,a.family_name
,a.version_id
,a.file_time
,a.file_id
,a.switch_name
,a.num_records
,a.accesspointnameni
,a.apnselectionmode
,a.causeforrecclosing
,a.ccrequestnumber
,a.ccsrealm
,a.chargingcharacteristics
,a.chargingid
,a.chchselectionmode
,a.creditcontrolfailurereport
,a.creditcontrolsessionid
,a.cunt1
,a.datavolumedownlink
,a.datavolumeuplink
,a.duration
,a.dynamicaddressflag
,a.endtime
,a.etsiaddress
,a.externalchargingid
,a.ggsnaddress
,a.identifier
,a.imssignalingcontext
,a.inactivity
,a.listoftrafficvolumes
,a.listofuritimestamps
,a.localsequencenumber
,a.method
,a.mstimezone
,a.nodeid
,a.pcsrealm
,a.pdptype
,a.policycontrolfailurereport
,a.policycontrolsessionid
,a.ratinggroup
,a.rattype
,a.recordopeningtime
,a.recordsequencenumber
,a.recordtype
,a.resolution
,a.rulespaceid
,a.servedimeisv
,a.servedimsi
,split(SUBSTR((a.servedmsisdn),3,12),'') servedmsisdn
--,a.servedmsisdn
,a.servedpdpaddress_iptextv6address
,a.servedpdpaddress_iptextv4address
,a.servedpdpaddress_ipbinv6address
,a.servedpdpaddress_ipbinv4address
,a.serviceidentifier
,a.servicespecificunits
,a.sgsnaddress
,a.sgsnplmnidentifier
,a.significance
,a.starttime
,a.uri
,a.uridatavolumedownlink
,a.uridatavolumeuplink
,a.uriidentifier
,a.usercategory
,a.userlocationinformation
,a.list_of_service_data
,a.servingnodetype
,a.p_gwplmnidentifier
,a.stoptime
,a.pdn_connection_id
,a.3gpp2_userlocationinformation
,a.served_pdp_pdn_addressext
,a.file_date FROM cdrdm.pgw1215raw a ${hiveconf:WHERE_clause}; 

drop table if exists cdrdm.pgw1215raw_non_pii_temp;

create table if not exists cdrdm.pgw1215raw_non_pii_temp like cdrdm.pgw1215raw_non_pii;

INSERT INTO cdrdm.pgw1215raw_non_pii_temp PARTITION(file_date)
SELECT a.file_name
,a.record_start
,a.record_end
,a.record_type
,a.family_name
,a.version_id
,a.file_time
,a.file_id
,a.switch_name
,a.num_records
,a.accesspointnameni
,a.apnselectionmode
,a.causeforrecclosing
,a.ccrequestnumber
,a.ccsrealm
,a.chargingcharacteristics
,a.chargingid
,a.chchselectionmode
,a.creditcontrolfailurereport
,a.creditcontrolsessionid
,a.cunt1
,a.datavolumedownlink
,a.datavolumeuplink
,a.duration
,a.dynamicaddressflag
,a.endtime
,a.etsiaddress
,a.externalchargingid
,a.ggsnaddress
,a.identifier
,a.imssignalingcontext
,a.inactivity
,a.listoftrafficvolumes
,a.listofuritimestamps
,a.localsequencenumber
,a.method
,a.mstimezone
,a.nodeid
,a.pcsrealm
,a.pdptype
,a.policycontrolfailurereport
,a.policycontrolsessionid
,a.ratinggroup
,a.rattype
,a.recordopeningtime
,a.recordsequencenumber
,a.recordtype
,a.resolution
,a.rulespaceid
,default.tokenize(a.servedimeisv, 'K10') as servedimeisv -- ,a.servedimeisv
,default.tokenize(a.servedimsi, 'K10') as servedimsi  --,a.servedimsi
,default.tokenize(concat(subs_array[0],subs_array[3],subs_array[2],subs_array[5],subs_array[4],subs_array[7],subs_array[6],subs_array[9],subs_array[8],subs_array[11]),'K2') as servedmsisdn
--,default.tokenize(a.servedmsisdn, 'K2') as servedmsisdn --,a.servedmsisdn 
,a.servedpdpaddress_iptextv6address
,a.servedpdpaddress_iptextv4address
,a.servedpdpaddress_ipbinv6address
,a.servedpdpaddress_ipbinv4address
,a.serviceidentifier
,a.servicespecificunits
,a.sgsnaddress
,a.sgsnplmnidentifier
,a.significance
,a.starttime
,a.uri
,a.uridatavolumedownlink
,a.uridatavolumeuplink
,a.uriidentifier
,a.usercategory
,a.userlocationinformation
,a.list_of_service_data
,a.servingnodetype
,a.p_gwplmnidentifier
,a.stoptime
,a.pdn_connection_id
,a.3gpp2_userlocationinformation
,a.served_pdp_pdn_addressext
,a.file_date
FROM ext_cdrdm.pgw1215raw_non_pii_temp a;

INSERT INTO cdrdm.pgw1215raw_non_pii PARTITION(file_date)
SELECT * FROM cdrdm.pgw1215raw_non_pii_temp;


TRUNCATE TABLE ext_cdrdm.pgw1215raw_non_pii_temp;
