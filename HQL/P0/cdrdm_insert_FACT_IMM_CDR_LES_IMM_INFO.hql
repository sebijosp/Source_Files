use cdrdm;
set hive.enforce.bucketing=true;
SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.min.partition.factor=0.25;
set hive.tez.max.partition.factor=2.0;
SET mapred.max.split.size=1000000;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.exec.parallel=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.dbclass=fs;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.exec.max.dynamic.partitions.pernode=15000;
set hive.exec.max.dynamic.partitions=15000;
set hive.exec.reducers.bytes.per.reducer=10432;
set tez.shuffle-vertex-manager.min-src-fraction=0.25;
set tez.shuffle-vertex-manager.max-src-fraction=0.75;
set tez.runtime.report.partition.stats=true;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.optimize.index.filter=true;
set hive.optimize.ppd.storage=true;

drop table if exists cdrdm.LES_IMM_INFO_STEP1;
create table cdrdm.LES_IMM_INFO_STEP1 stored as orc as
SELECT IF((CASE   WHEN  A.lst10accessdomain = '20' THEN A.lst10accessdomain  WHEN  A.lst9accessdomain = '20' THEN A.lst9accessdomain  WHEN  A.lst9accessdomain = '20' THEN A.lst8accessdomain  WHEN  A.lst7accessdomain = '20' THEN A.lst7accessdomain  WHEN  A.lst6accessdomain = '20' THEN A.lst6accessdomain  WHEN  A.lst5accessdomain = '20' THEN A.lst5accessdomain  WHEN  A.lst4accessdomain = '20' THEN A.lst4accessdomain  WHEN  A.lst3accessdomain = '20' THEN A.lst3accessdomain  WHEN  A.lst2accessdomain = '20' THEN A.lst2accessdomain  WHEN  A.lst1accessdomain = '20' THEN A.lst1accessdomain ELSE NULL END) = '20','YES','NO') as IsWifiUsage, A.recordtype,A.retransmission,A.sipmethod,A.roleofnode,A.nodeaddress,A.sessionid,A.callingpartyaddress,A.calledpartyaddress,A.srvcrequesttimestamp,A.srvcdeliverystarttimestamp,A.srvcdeliveryendtimestamp,A.recordopeningtime,A.recordclosuretime,A.srvcrequestdttmnrml,A.srvcdeliverystartdttmnrml,A.srvcdeliveryenddttmnrml,A.chargeableduration,A.tmfromsiprqststartofchrgng,A.interoperatoridentifiers,A.localrecordsequencenumber,A.partialoutputrecordnumber,A.causeforrecordclosing,A.acrstartlost,A.acrinterimlost,A.acrstoplost,A.imsilost,A.acrsccasstartlost,A.acrsccasinterimlost,A.acrsccasstoplost,A.imschargingidentifier,A.lstofsdpmediacomponents,A.lstofnrmlmediac1chgtime,A.lstofnrmlmediac1chgtimenor,A.lstofnrmlmediac1desctype1,A.lstofnrmlmediac1desccodec1,A.lstofnrmlmediac1descbndw1,A.lstofnrmlmediac1desctype2,A.lstofnrmlmediac1desccodec2,A.lstofnrmlmediac1descbndw2,A.lstofnrmlmediac1desctype3,A.lstofnrmlmediac1desccodec3,A.lstofnrmlmediac1descbndw3,A.lstofnrmlmediac1medinitflg,A.lstofnrmlmediac2chgtime,A.lstofnrmlmediac2chgtimenor,A.lstofnrmlmediac2desctype1,A.lstofnrmlmediac2desccodec1,A.lstofnrmlmediac2descbndw1,A.lstofnrmlmediac2desctype2,A.lstofnrmlmediac2desccodec2,A.lstofnrmlmediac2descbndw2,A.lstofnrmlmediac2desctype3,A.lstofnrmlmediac2desccodec3,A.lstofnrmlmediac2descbndw3,A.lstofnrmlmediac2medinitflg,A.lstofnrmlmediac3chgtime,A.lstofnrmlmediac3chgtimenor,A.lstofnrmlmediac3desctype1,A.lstofnrmlmediac3desccodec1,A.lstofnrmlmediac3descbndw1,A.lstofnrmlmediac3desctype2,A.lstofnrmlmediac3desccodec2,A.lstofnrmlmediac3descbndw2,A.lstofnrmlmediac3desctype3,A.lstofnrmlmediac3desccodec3,A.lstofnrmlmediac3descbndw3,A.lstofnrmlmediac3medinitflg,A.lstofnrmlmediac4chgtime,A.lstofnrmlmediac4chgtimenor,A.lstofnrmlmediac4desctype1,A.lstofnrmlmediac4desccodec1,A.lstofnrmlmediac4descbndw1,A.lstofnrmlmediac4desctype2,A.lstofnrmlmediac4desccodec2,A.lstofnrmlmediac4descbndw2,A.lstofnrmlmediac4desctype3,A.lstofnrmlmediac4desccodec3,A.lstofnrmlmediac4descbndw3,A.lstofnrmlmediac4medinitflg,A.lstofnrmlmediac5chgtime,A.lstofnrmlmediac5chgtimenor,A.lstofnrmlmediac5desctype1,A.lstofnrmlmediac5desccodec1,A.lstofnrmlmediac5descbndw1,A.lstofnrmlmediac5desctype2,A.lstofnrmlmediac5desccodec2,A.lstofnrmlmediac5descbndw2,A.lstofnrmlmediac5desctype3,A.lstofnrmlmediac5desccodec3,A.lstofnrmlmediac5descbndw3,A.lstofnrmlmediac5medinitflg,A.lstofnrmlmediacomponents6,A.lstofnrmlmediacomponents7,A.lstofnrmlmediacomponents8,A.lstofnrmlmediacomponents9,A.lstofnrmlmediacomponents10,A.lstofnrmlmediacomponents11,A.lstofnrmlmediacomponents12,A.lstofnrmlmediacomponents13,A.lstofnrmlmediacomponents14,A.lstofnrmlmediacomponents15,A.lstofnrmlmediacomponents16,A.lstofnrmlmediacomponents17,A.lstofnrmlmediacomponents18,A.lstofnrmlmediacomponents19,A.lstofnrmlmediacomponents20,A.lstofnrmlmediacomponents21,A.lstofnrmlmediacomponents22,A.lstofnrmlmediacomponents23,A.lstofnrmlmediacomponents24,A.lstofnrmlmediacomponents25,A.lstofnrmlmediacomponents26,A.lstofnrmlmediacomponents27,A.lstofnrmlmediacomponents28,A.lstofnrmlmediacomponents29,A.lstofnrmlmediacomponents30,A.lstofnrmlmediacomponents31,A.lstofnrmlmediacomponents32,A.lstofnrmlmediacomponents33,A.lstofnrmlmediacomponents34,A.lstofnrmlmediacomponents35,A.lstofnrmlmediacomponents36,A.lstofnrmlmediacomponents37,A.lstofnrmlmediacomponents38,A.lstofnrmlmediacomponents39,A.lstofnrmlmediacomponents40,A.lstofnrmlmediacomponents41,A.lstofnrmlmediacomponents42,A.lstofnrmlmediacomponents43,A.lstofnrmlmediacomponents44,A.lstofnrmlmediacomponents45,A.lstofnrmlmediacomponents46,A.lstofnrmlmediacomponents47,A.lstofnrmlmediacomponents48,A.lstofnrmlmediacomponents49,A.lstofnrmlmediacomponents50,A.lstofnrmlmedcompts1150,A.ggsnaddress,A.servicereasonreturncode,A.lstofmessagebodies,A.recordextension,A.expires,A.event,A.lst1accessnetworkinfo,A.lst1accessdomain,A.lst1accesstype,A.lst1locationinfotype,A.lst1changetime,A.lst1changetimenormalized,A.lst2accessnetworkinfo,A.lst2accessdomain,A.lst2accesstype,A.lst2locationinfotype,A.lst2changetime,A.lst2changetimenormalized,A.lst3accessnetworkinfo,A.lst3accessdomain,A.lst3accesstype,A.lst3locationinfotype,A.lst3changetime,A.lst3changetimenormalized,A.lst4accessnetworkinfo,A.lst4accessdomain,A.lst4accesstype,A.lst4locationinfotype,A.lst4changetime,A.lst4changetimenormalized,A.lst5accessnetworkinfo,A.lst5accessdomain,A.lst5accesstype,A.lst5locationinfotype,A.lst5changetime,A.lst5changetimenormalized,A.lst6accessnetworkinfo,A.lst6accessdomain,A.lst6accesstype,A.lst6locationinfotype,A.lst6changetime,A.lst6changetimenormalized,A.lst7accessnetworkinfo,A.lst7accessdomain,A.lst7accesstype,A.lst7locationinfotype,A.lst7changetime,A.lst7changetimenormalized,A.lst8accessnetworkinfo,A.lst8accessdomain,A.lst8accesstype,A.lst8locationinfotype,A.lst8changetime,A.lst8changetimenormalized,A.lst9accessnetworkinfo,A.lst9accessdomain,A.lst9accesstype,A.lst9locationinfotype,A.lst9changetime,A.lst9changetimenormalized,A.lst10accessnetworkinfo,A.lst10accessdomain,A.lst10accesstype,A.lst10locationinfotype,A.lst10changetime,A.lst10changetimenormalized,A.servicecontextid,A.subscribere164,A.subscriberno,A.imsi,A.imei,A.subsipuri,A.nai,A.subprivate,A.subservedpartydeviceid,A.lstofearlysdpmediacmpnts,A.imscommserviceidentifier,A.numberportabilityrouting,A.carrierselectrouting,A.sessionpriority,A.requestedpartyaddress,A.lstofcalledassertedid,A.mmtelinfoanalyzedcalltype,A.mtlinfocalledasrtidprsntsts,A.mtlinfocllgprtyaddrprsntsts,A.mmtelinfoconferenceid,A.mmtelinfodialaroundindctr,A.mmtelinforelatedicid,A.mmtelsplmtrysrvcinfo1id,A.mmtelsplmtrysrvcinfo1act,A.mmtelsplmtrysrvcinfo1redir,A.mmtelsplmtrysrvcinfo2id,A.mmtelsplmtrysrvcinfo2act,A.mmtelsplmtrysrvcinfo2redir,A.mmtelinfosplmtrysrvcinfo3,A.mmtelinfosplmtrysrvcinfo4,A.mmtelinfosplmtrysrvcinfo5,A.mmtelinfosplmtrysrvcinfo6,A.mmtelinfosplmtrysrvcinfo7,A.mmtelinfosplmtrysrvcinfo8,A.mmtelinfosplmtrysrvcinfo9,A.mmtelinfosplmtrysrvcinfo10,A.mobilestationroamingnumber,A.teleservicecode,A.tariffclass,A.pvisitednetworkid,A.srvcrequestdttmnrml_date,CASE   WHEN A.lst10accessnetworkinfo IS not NULL THEN A.lst10accessnetworkinfo  WHEN A.lst9accessnetworkinfo IS not NULL THEN A.lst9accessnetworkinfo WHEN A.lst8accessnetworkinfo IS not NULL THEN A.lst8accessnetworkinfo WHEN A.lst7accessnetworkinfo IS not NULL THEN A.lst7accessnetworkinfo WHEN A.lst6accessnetworkinfo IS not NULL THEN A.lst6accessnetworkinfo  WHEN A.lst5accessnetworkinfo IS not NULL THEN A.lst5accessnetworkinfo WHEN A.lst4accessnetworkinfo IS not NULL THEN A.lst4accessnetworkinfo  WHEN A.lst3accessnetworkinfo IS not NULL THEN A.lst3accessnetworkinfo WHEN A.lst2accessnetworkinfo IS not NULL THEN A.lst2accessnetworkinfo else null END AS LAST_CELL,
UPPER(CONCAT(SUBSTR(A.Lst1AccessNetworkInfo,1,3),SUBSTR(A.Lst1AccessNetworkInfo,5,3),SUBSTR(A.Lst1AccessNetworkInfo,9,4),REGEXP_EXTRACT(TRIM(SUBSTR(A.Lst1AccessNetworkInfo,14)), '(0*)(.*)', 2))) AS FIRST_CELL
FROM cdrdm.FACT_IMM_CDR_STG A;
INSERT into table cdrdm.LES_IMM_INFO PARTITION (srvcrequestdttmnrml_date)
SELECT a.IsWifiUsage,a.RecordType,a.Retransmission,a.SIPMethod,a.RoleOfNode,a.NodeAddress,a.SessionId,a.CallingPartyAddress,a.CalledPartyAddress,a.SrvcRequestTimeStamp,a.SrvcDeliveryStartTimeStamp,a.SrvcDeliveryEndTimeStamp,a.RecordOpeningTime,a.RecordClosureTime,a.SrvcRequestDttmNrml,a.SrvcDeliveryStartDttmNrml,a.SrvcDeliveryEndDttmNrml,a.ChargeableDuration,a.TmFromSipRqstStartOfChrgng,a.InterOperatorIdentifiers,a.LocalRecordSequenceNumber,a.PartialOutputRecordNumber,a.CauseForRecordClosing,a.ACRStartLost,a.ACRInterimLost,a.ACRStopLost,a.IMSILost,a.ACRSCCASStartLost,a.ACRSCCASInterimLost,a.ACRSCCASStopLost,a.IMSChargingIdentifier,a.LstOfSDPMediaComponents,a.LstOfNrmlMediaC1ChgTime,a.LstOfNrmlMediaC1ChgTimeNor,a.LstOfNrmlMediaC1DescType1,a.LstOfNrmlMediaC1DescCodec1,a.LstOfNrmlMediaC1DescBndW1,a.LstOfNrmlMediaC1DescType2,a.LstOfNrmlMediaC1DescCodec2,a.LstOfNrmlMediaC1DescBndW2,a.LstOfNrmlMediaC1DescType3,a.LstOfNrmlMediaC1DescCodec3,a.LstOfNrmlMediaC1DescBndW3,a.LstOfNrmlMediaC1medInitFlg,a.LstOfNrmlMediaC2ChgTime,a.LstOfNrmlMediaC2ChgTimeNor,a.LstOfNrmlMediaC2DescType1,a.LstOfNrmlMediaC2DescCodec1,a.LstOfNrmlMediaC2DescBndW1,a.LstOfNrmlMediaC2DescType2,a.LstOfNrmlMediaC2DescCodec2,a.LstOfNrmlMediaC2DescBndW2,a.LstOfNrmlMediaC2DescType3,a.LstOfNrmlMediaC2DescCodec3,a.LstOfNrmlMediaC2DescBndW3,a.LstOfNrmlMediaC2medInitFlg,a.LstOfNrmlMediaC3ChgTime,a.LstOfNrmlMediaC3ChgTimeNor,a.LstOfNrmlMediaC3DescType1,a.LstOfNrmlMediaC3DescCodec1,a.LstOfNrmlMediaC3DescBndW1,a.LstOfNrmlMediaC3DescType2,a.LstOfNrmlMediaC3DescCodec2,a.LstOfNrmlMediaC3DescBndW2,a.LstOfNrmlMediaC3DescType3,a.LstOfNrmlMediaC3DescCodec3,a.LstOfNrmlMediaC3DescBndW3,a.LstOfNrmlMediaC3medInitFlg,a.LstOfNrmlMediaC4ChgTime,a.LstOfNrmlMediaC4ChgTimeNor,a.LstOfNrmlMediaC4DescType1,a.LstOfNrmlMediaC4DescCodec1,a.LstOfNrmlMediaC4DescBndW1,a.LstOfNrmlMediaC4DescType2,a.LstOfNrmlMediaC4DescCodec2,a.LstOfNrmlMediaC4DescBndW2,a.LstOfNrmlMediaC4DescType3,a.LstOfNrmlMediaC4DescCodec3,a.LstOfNrmlMediaC4DescBndW3,a.LstOfNrmlMediaC4medInitFlg,a.LstOfNrmlMediaC5ChgTime,a.LstOfNrmlMediaC5ChgTimeNor,a.LstOfNrmlMediaC5DescType1,a.LstOfNrmlMediaC5DescCodec1,a.LstOfNrmlMediaC5DescBndW1,a.LstOfNrmlMediaC5DescType2,a.LstOfNrmlMediaC5DescCodec2,a.LstOfNrmlMediaC5DescBndW2,a.LstOfNrmlMediaC5DescType3,a.LstOfNrmlMediaC5DescCodec3,a.LstOfNrmlMediaC5DescBndW3,a.LstOfNrmlMediaC5medInitFlg,a.LstOfNrmlMediaComponents6,a.LstOfNrmlMediaComponents7,a.LstOfNrmlMediaComponents8,a.LstOfNrmlMediaComponents9,a.LstOfNrmlMediaComponents10,a.LstOfNrmlMediaComponents11,a.LstOfNrmlMediaComponents12,a.LstOfNrmlMediaComponents13,a.LstOfNrmlMediaComponents14,a.LstOfNrmlMediaComponents15,a.LstOfNrmlMediaComponents16,a.LstOfNrmlMediaComponents17,a.LstOfNrmlMediaComponents18,a.LstOfNrmlMediaComponents19,a.LstOfNrmlMediaComponents20,a.LstOfNrmlMediaComponents21,a.LstOfNrmlMediaComponents22,a.LstOfNrmlMediaComponents23,a.LstOfNrmlMediaComponents24,a.LstOfNrmlMediaComponents25,a.LstOfNrmlMediaComponents26,a.LstOfNrmlMediaComponents27,a.LstOfNrmlMediaComponents28,a.LstOfNrmlMediaComponents29,a.LstOfNrmlMediaComponents30,a.LstOfNrmlMediaComponents31,a.LstOfNrmlMediaComponents32,a.LstOfNrmlMediaComponents33,a.LstOfNrmlMediaComponents34,a.LstOfNrmlMediaComponents35,a.LstOfNrmlMediaComponents36,a.LstOfNrmlMediaComponents37,a.LstOfNrmlMediaComponents38,a.LstOfNrmlMediaComponents39,a.LstOfNrmlMediaComponents40,a.LstOfNrmlMediaComponents41,a.LstOfNrmlMediaComponents42,a.LstOfNrmlMediaComponents43,a.LstOfNrmlMediaComponents44,a.LstOfNrmlMediaComponents45,a.LstOfNrmlMediaComponents46,a.LstOfNrmlMediaComponents47,a.LstOfNrmlMediaComponents48,a.LstOfNrmlMediaComponents49,a.LstOfNrmlMediaComponents50,a.LstOfNrmlMedCompts1150,a.GGSNAddress,a.ServiceReasonReturnCode,a.LstOfMessageBodies,a.RecordExtension,a.Expires,a.Event,a.Lst1AccessNetworkInfo,a.Lst1AccessDomain,a.Lst1AccessType,a.Lst1LocationInfoType,a.Lst1ChangeTime,a.Lst1ChangeTimeNormalized,a.Lst2AccessNetworkInfo,a.Lst2AccessDomain,a.Lst2AccessType,a.Lst2LocationInfoType,a.Lst2ChangeTime,a.Lst2ChangeTimeNormalized,a.Lst3AccessNetworkInfo,a.Lst3AccessDomain,a.Lst3AccessType,a.Lst3LocationInfoType,a.Lst3ChangeTime,a.Lst3ChangeTimeNormalized,a.Lst4AccessNetworkInfo,a.Lst4AccessDomain,a.Lst4AccessType,a.Lst4LocationInfoType,a.Lst4ChangeTime,a.Lst4ChangeTimeNormalized,a.Lst5AccessNetworkInfo,a.Lst5AccessDomain,a.Lst5AccessType,a.Lst5LocationInfoType,a.Lst5ChangeTime,a.Lst5ChangeTimeNormalized,a.Lst6AccessNetworkInfo,a.Lst6AccessDomain,a.Lst6AccessType,a.Lst6LocationInfoType,a.Lst6ChangeTime,a.Lst6ChangeTimeNormalized,a.Lst7AccessNetworkInfo,a.Lst7AccessDomain,a.Lst7AccessType,a.Lst7LocationInfoType,a.Lst7ChangeTime,a.Lst7ChangeTimeNormalized,a.Lst8AccessNetworkInfo,a.Lst8AccessDomain,a.Lst8AccessType,a.Lst8LocationInfoType,a.Lst8ChangeTime,a.Lst8ChangeTimeNormalized,a.Lst9AccessNetworkInfo,a.Lst9AccessDomain,a.Lst9AccessType,a.Lst9LocationInfoType,a.Lst9ChangeTime,a.Lst9ChangeTimeNormalized,a.Lst10AccessNetworkInfo,a.Lst10AccessDomain,a.Lst10AccessType,a.Lst10LocationInfoType,a.Lst10ChangeTime,a.Lst10ChangeTimeNormalized,a.ServiceContextID,a.SubscriberE164,a.SubscriberNo,a.IMSI,a.IMEI,a.SubSIPURI,a.NAI,a.SubPrivate,a.SubServedPartyDeviceID,a.LstOfEarlySDPMediaCmpnts,a.IMSCommServiceIdentifier,a.NumberPortabilityRouting,a.CarrierSelectRouting,a.SessionPriority,a.RequestedPartyAddress,a.LstOfCalledAssertedID,a.MMTelInfoAnalyzedCallType,a.MTlInfoCalledAsrtIDPrsntSts,a.MTlInfoCllgPrtyAddrPrsntSts,a.MMTelInfoConferenceId,a.MMTelInfoDialAroundIndctr,a.MMTelInfoRelatedICID,a.MMTelSplmtrySrvcInfo1ID,a.MMTelSplmtrySrvcInfo1Act,a.MMTelSplmtrySrvcInfo1Redir,a.MMTelSplmtrySrvcInfo2ID,a.MMTelSplmtrySrvcInfo2Act,a.MMTelSplmtrySrvcInfo2Redir,a.MMTelInfoSplmtrySrvcInfo3,a.MMTelInfoSplmtrySrvcInfo4,a.MMTelInfoSplmtrySrvcInfo5,a.MMTelInfoSplmtrySrvcInfo6,a.MMTelInfoSplmtrySrvcInfo7,a.MMTelInfoSplmtrySrvcInfo8,a.MMTelInfoSplmtrySrvcInfo9,a.MMTelInfoSplmtrySrvcInfo10,a.MobileStationRoamingNumber,a.TeleServiceCode,a.TariffClass,a.pVisitedNetworkID,a.first_cell,UPPER(CONCAT(SUBSTR(a.LAST_CELL,1,3),SUBSTR(a.LAST_CELL,5,3),SUBSTR(a.LAST_CELL,9,4),REGEXP_EXTRACT(TRIM(SUBSTR(a.LAST_CELL,14)), '(0*)(.*)', 2))) as last_cell,A.srvcrequestdttmnrml_date
FROM cdrdm.LES_IMM_INFO_STEP1 a ;

INSERT INTO CDRDM.LES_FC_XREF PARTITION(SRC_TABLE_NAME = 'LES_IMM_INFO',CALL_TIMESTAMP_DATE)
SELECT DISTINCT A.first_Cell,SUBSTR(A.recordopeningtime,1,10), A.srvcrequestdttmnrml_date
 FROM CDRDM.LES_IMM_INFO_STEP1 A WHERE  A.first_Cell IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM CDRDM.LES_FC_XREF B
 WHERE B.SRC_TABLE_NAME = 'LES_IMM_INFO'
 AND B.CALL_TIMESTAMP_DATE = A.srvcrequestdttmnrml_date
 AND B.FIRST_Cell = A.FIRST_Cell)
 ;
 
 INSERT INTO CDRDM.LES_LC_XREF PARTITION(SRC_TABLE_NAME = 'LES_IMM_INFO',CALL_TIMESTAMP_DATE)
 SELECT DISTINCT UPPER(CONCAT(SUBSTR(a.LAST_CELL,1,3),SUBSTR(a.LAST_CELL,5,3),SUBSTR(a.LAST_CELL,9,4),REGEXP_EXTRACT(TRIM(SUBSTR(a.LAST_CELL,14)), '(0*)(.*)', 2))),SUBSTR(A.recordopeningtime,1,10), A.srvcrequestdttmnrml_date
 FROM CDRDM.LES_IMM_INFO_STEP1 A WHERE  A.Last_Cell IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM CDRDM.LES_LC_XREF B
 WHERE B.SRC_TABLE_NAME = 'LES_IMM_INFO'
 AND B.CALL_TIMESTAMP_DATE = A.srvcrequestdttmnrml_date
 AND B.Last_Cell = A.Last_Cell)
 ;

INSERT OVERWRITE table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_FC_STG  PARTITION (srvcrequestdttmnrml_date)
SELECT  A.site, A.cell, A.enodeb, A.cgi, upper(trim(A.cgi_hex)) as cgi_hex, A.site_name, A.original_i, A.antenna_ty, A.azimuth, A.beamwidth, A.x, A.y, A.longitude, A.latitude, A.address, A.city, A.province, A.arfcndl, A.bcchno, A.locationco, A.bsic, A.primaryscr, A.file_type, A.insert_timestamp, A.file_date,B.FIRST_CELL,B.CALL_TIMESTAMP_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_FC_XREF B ON B.SRC_TABLE_NAME = 'LES_IMM_INFO' AND  UPPER(TRIM(A.CGI_HEX)) = B.FIRST_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

INSERT OVERWRITE table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_lC_STG  PARTITION (srvcrequestdttmnrml_date)
SELECT  A.site, A.cell, A.enodeb, A.cgi, upper(trim(A.cgi_hex)) as cgi_hex, A.site_name, A.original_i, A.antenna_ty, A.azimuth, A.beamwidth, A.x, A.y, A.longitude, A.latitude, A.address, A.city, A.province, A.arfcndl, A.bcchno, A.locationco, A.bsic, A.primaryscr, A.file_type, A.insert_timestamp, A.file_date,B.last_CELL,B.CALL_TIMESTAMP_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_LC_XREF B ON B.SRC_TABLE_NAME = 'LES_IMM_INFO' AND UPPER(TRIM(A.CGI_HEX)) = B.last_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

INSERT INTO TABLE cdrdm.fact_obroamrpts_cdr 
PARTITION(cday,service_type,call_distance_type)
SELECT
  srvcrequestdttmnrml_date 
    AS CALLDAY,
  DATE_FORMAT(SUBSTR(srvcrequestdttmnrml_date,1,10),'MMMMM') 
    AS CALLMONTH, 
  splitcdr.CALLSTARTTIME 
    AS CALLSTARTTIME,
  splitcdr.CALLENDTIME
    AS CALLENDTIME,
  CASE 
      WHEN SUBSTR(imsi,0,6) = '302720'
          THEN 'Rogers' 
      WHEN SUBSTR(imsi,0,6) = '302370'  
          THEN 'Fido'
      ELSE imsi 
  END 
    AS BRAND,
  NVL(TRIM(imsi),'?') 
    AS IMSI,
  NVL(TRIM(imei),'?') 
    AS IMEI,
  subscriberno 
    AS MSISDN,
  NVL(pll.plmn_tadig,'')  
    AS VPLMNTADIGCode,
  CASE 
    WHEN COALESCE(lst1accessdomain,
                  lst2accessdomain,
                  lst3accessdomain) = '20'
      THEN COALESCE(wificountry.country_name,
                    pll.country_name,
                    '?') 
    ELSE
      NVL(pll.country_name,'?') 
  END 
    AS VISITEDCOUNTRY,
  NVL(pll.plmn_name,'') 
    AS VPLMNNAME,
  CASE
    WHEN lst1accessdomain = '2'
      THEN
        CASE  
          WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,6),'-',''),'0')
          ELSE 
            REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,7),'-','')
        END
    WHEN lst2accessdomain = '2'
      THEN 
        CASE
          WHEN SUBSTR(lst2accessnetworkinfo,7,1) = '-' 
            THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,1,6),'-',''),'0')
          ELSE 
            REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,1,7),'-','')
        END
    WHEN lst3accessdomain = '2'
      THEN 
        CASE
          WHEN SUBSTR(lst3accessnetworkinfo,7,1) = '-' 
            THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,1,6),'-',''),'0')
          ELSE 
            REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,1,7),'-','')
        END
    ELSE ''
  END 
    AS MCCMNC,
  splitcdr.CALLTYPE
    AS CALLTYPE,
  splitcdr.SERVICE
      AS SERVICE,    
  CASE
    WHEN
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
      COALESCE(npanxx.country_name,
                specialnum.specialnumber,
                fivedigitprfx.country_name,
                fourdigitprfx.country_name,
                '?')
    ELSE
        COALESCE(threedigitprfx.country_name,
                twodigitprfx.country_name,
                onedigitprfx.country_name,
                '?')
  END 
    AS CALLEDCOUNTRY,
  CASE 
    WHEN 
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
        COALESCE(IF(ISNOTNULL(npanxx.country_name),'1',NULL),
                  IF(ISNOTNULL(specialnum.specialnumber),'1',NULL),
                  fivedigitprfx.plmn_country_code,
                  fourdigitprfx.plmn_country_code,
                  '?')
    ELSE 
        COALESCE(threedigitprfx.plmn_country_code,
                twodigitprfx.plmn_country_code,
                onedigitprfx.plmn_country_code,
                '?')
  END 
    AS COUNTRYCODE,    
  CAST(calledpartyaddress AS BIGINT) 
    AS CALLEDNUMBER,
  requestedpartyaddress 
    AS DIALEDNUMBER,
  splitcdr.CALLDURATIONSECS 
    AS CALLDURATIONSECS,
  splitcdr.CALLDURATIONMINS 
    AS CALLDURATIONMINS,
   '' 
    AS DATAVOLUME,
   '' 
    AS CALL_TYPE_LEVEL_2,
   '' 
    AS ACCESS_POINT_NAME_NI,
   '1' 
    AS NUMBEROFEVENTS,
  CASE
    WHEN lst1accessdomain = '2'
      THEN
        CASE
          WHEN 
            (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-','') = "" 
              OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
          WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-',''),16,10)
        END
    WHEN lst2accessdomain = '2'
      THEN
        CASE
          WHEN 
            (REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,14,7),'-','') = "" 
              OR REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
          WHEN SUBSTR(lst2accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,13,7),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,14,7),'-',''),16,10)
        END
    WHEN lst3accessdomain = '3'
      THEN
        CASE
          WHEN 
            (REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,14,7),'-','') = "" 
              OR REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
          WHEN SUBSTR(lst3accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,13,7),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,14,7),'-',''),16,10)
        END
    ELSE ''
  END 
    AS SERVINGCELLID,
  CASE
    WHEN lst1accessdomain = '2'
      THEN
        CASE
          WHEN (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-','') = "" 
                OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
          WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-',''),16,10)
        END
    WHEN lst2accessdomain = '2'
      THEN
        CASE
          WHEN (REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,9,4),'-','') = "" 
                OR REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
          WHEN SUBSTR(lst2accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,8,4),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,9,4),'-',''),16,10)
          END
    WHEN lst3accessdomain = '3'
      THEN
        CASE
          WHEN (REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,9,4),'-','') = "" 
                OR REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
          WHEN SUBSTR(lst3accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,8,4),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,9,4),'-',''),16,10)
          END
    ELSE ''
  END
    AS SERVINGLAC,
  CURRENT_DATE 
    AS INSERTDATE,
  CURRENT_TIMESTAMP 
    AS INSERTTIME,
  file_name 
    AS FILE_NAME,
  record_type 
    AS RECORD_TYPE,
  family_name 
    AS FAMILY_NAME,
  version_id 
    AS VERSION_ID,
  file_time 
    AS FILE_TIME,
  file_id 
    AS FILE_ID,
  switch_name 
    AS SWITCH_NAME,
CASE
WHEN factcdr.mmtelsplmtrysrvcinfo1id='9444' or
factcdr.mmtelsplmtrysrvcinfo2id='9444' or
factcdr.mmtelsplmtrysrvcinfo3id='9444' or
factcdr.mmtelsplmtrysrvcinfo4id='9444' or
factcdr.mmtelsplmtrysrvcinfo5id='9444' or
factcdr.mmtelsplmtrysrvcinfo6id='9444' or
factcdr.mmtelsplmtrysrvcinfo7id='9444' or
factcdr.mmtelsplmtrysrvcinfo8id='9444' or
factcdr.mmtelsplmtrysrvcinfo9id='9444' or
factcdr.mmtelsplmtrysrvcinfo10id='9444' 
THEN 'Y'
ELSE NULL
END AS CALLCORRECTED,
CAST(REGEXP_REPLACE(SUBSTR(srvcrequestdttmnrml_date,1,10),'-','') AS INT) AS CDAY,
'4G' AS SERVICE_TYPE,
'ROAMING' AS CALL_DISTANCE_TYPE
FROM 
(
        SELECT
        srvcrequestdttmnrml_date,
        srvcdeliverystarttimestamp,
        srvcdeliveryendtimestamp,
        chargeableduration,
        lstofnrmlmediac1chgtime,
        lstofnrmlmediac2chgtime,
        lstofnrmlmediac3chgtime,
        lstofnrmlmediac4chgtime,
        lstofnrmlmediac5chgtime,
        lstofnrmlmediac1desctype1,
        lstofnrmlmediac2desctype1,
        lstofnrmlmediac2desctype2,
        lst1changetime,
        lst2changetime,
        imsi,
        imei,
        subscriberno,
        lst1accesstype,
        lst1accessnetworkinfo,
        lst1accessdomain,
        lst2accesstype,
        lst2accessnetworkinfo,
        lst2accessdomain,
        lst3accesstype,
        lst3accessnetworkinfo,
        lst3accessdomain,
        roleofnode,
        sipmethod,
        calledpartyaddress,
        requestedpartyaddress,
        file_name,
        record_type,
        family_name,
        version_id,
        file_time,
        file_id,
        switch_name,
        MMTelSplmtrySrvcInfo1ID,
        MMTelSplmtrySrvcInfo2ID,
        if(MMTelInfoSplmtrySrvcInfo3 is not NULL and length(MMTelInfoSplmtrySrvcInfo3) > 0,split(MMTelInfoSplmtrySrvcInfo3,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo3ID,
        if(MMTelInfoSplmtrySrvcInfo4 is not NULL and length(MMTelInfoSplmtrySrvcInfo4) > 0,split(MMTelInfoSplmtrySrvcInfo4,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo4ID,
        if(MMTelInfoSplmtrySrvcInfo5 is not NULL and length(MMTelInfoSplmtrySrvcInfo5) > 0,split(MMTelInfoSplmtrySrvcInfo5,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo5ID,
        if(MMTelInfoSplmtrySrvcInfo6 is not NULL and length(MMTelInfoSplmtrySrvcInfo6) > 0,split(MMTelInfoSplmtrySrvcInfo6,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo6ID,
        if(MMTelInfoSplmtrySrvcInfo7 is not NULL and length(MMTelInfoSplmtrySrvcInfo7) > 0,split(MMTelInfoSplmtrySrvcInfo7,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo7ID,
        if(MMTelInfoSplmtrySrvcInfo8 is not NULL and length(MMTelInfoSplmtrySrvcInfo8) > 0,split(MMTelInfoSplmtrySrvcInfo8,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo8ID,
        if(MMTelInfoSplmtrySrvcInfo9 is not NULL and length(MMTelInfoSplmtrySrvcInfo9) > 0,split(MMTelInfoSplmtrySrvcInfo9,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo9ID,
        if(MMTelInfoSplmtrySrvcInfo10 is not NULL and length(MMTelInfoSplmtrySrvcInfo10) > 0,split(MMTelInfoSplmtrySrvcInfo10,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo10ID
  FROM cdrdm.fact_imm_cdr_stg
  WHERE
    sipmethod='INVITE'
    AND
    (
        (--ViLTE and VoLTE CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='2' AND
                lst1accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst1accessnetworkinfo,1,6) <> '302-72' 
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='2' AND
                lst2accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst2accessnetworkinfo,1,6) <> '302-72'
            )
            OR
            (
                lst3accesstype='1' AND
                lst2accessdomain='2' AND
                lst3accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst3accessnetworkinfo,1,6) <> '302-72'
            )
        )
        OR
        (--ViWiFi and VoWiFi CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='20' AND
                lst1accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst1accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='20' AND
                lst2accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst2accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst3accesstype='1' AND
                lst3accessdomain='20' AND
                lst3accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst3accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
        )
    )
) factcdr
LEFT OUTER JOIN 
(
    SELECT * FROM 
    (
            SELECT 
                PL.*,
                ROW_NUMBER() OVER(PARTITION BY pl.plmn_code ORDER BY pl.effective_date desc) AS rown  
            FROM 
                ela_v21.plmn_settlement_gg pl
    ) AS pll 
    WHERE pll.rown=1
)  pll 
ON IF ( 
        LENGTH(REGEXP_REPLACE(REGEXP_EXTRACT(
                COALESCE(
                    IF((SUBSTR(lst3accessnetworkinfo,1,6)='FFFFFF'),NULL,lst3accessnetworkinfo),
                    IF((SUBSTR(lst2accessnetworkinfo,1,6)='FFFFFF'),NULL,lst2accessnetworkinfo),
                    IF((SUBSTR(lst1accessnetworkinfo,1,6)='FFFFFF'),NULL,lst1accessnetworkinfo)),'(.+?-.+?)-.+?'),'-',''))=6,
        REGEXP_REPLACE(REGEXP_EXTRACT(
                COALESCE(
                    IF((SUBSTR(lst3accessnetworkinfo,1,6)='FFFFFF'),NULL,lst3accessnetworkinfo),
                    IF((SUBSTR(lst2accessnetworkinfo,1,6)='FFFFFF'),NULL,lst2accessnetworkinfo),
                    IF((SUBSTR(lst1accessnetworkinfo,1,6)='FFFFFF'),NULL,lst1accessnetworkinfo)),'(.+?-.+?)-.+?'),'-',''),
        CONCAT(REGEXP_REPLACE(REGEXP_EXTRACT(
                COALESCE(
                    IF((SUBSTR(lst3accessnetworkinfo,1,6)='FFFFFF'),NULL,lst3accessnetworkinfo),
                    IF((SUBSTR(lst2accessnetworkinfo,1,6)='FFFFFF'),NULL,lst2accessnetworkinfo),
                    IF((SUBSTR(lst1accessnetworkinfo,1,6)='FFFFFF'),NULL,lst1accessnetworkinfo)),'(.+?-.+?)-.+?'),'-',''),'0')
 )=pll.plmn_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
              WHERE LENGTH(ps.plmn_country_code) = 5
        ) AS cntry
        WHERE cntry.rown=1
) fivedigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fivedigitprfx.plmn_country_code
LEFT OUTER JOIN
  (
      SELECT
          cntry.plmn_country_code,
          cntry.country_name
      FROM
          (
              SELECT
                  DISTINCT ps.plmn_country_code,
                  ps.country_name,
                  row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
              FROM
                  ela_v21.plmn_settlement_gg ps
                WHERE LENGTH(ps.plmn_country_code) = 4
          ) AS cntry
          WHERE cntry.rown=1
  ) fourdigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fourdigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE ps.plmn_country_code <> '1'
                AND LENGTH(ps.plmn_country_code) = 3
        ) AS cntry
        WHERE cntry.rown=1
) threedigitprfx
ON SUBSTR(calledpartyaddress,1,3) = threedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 2
        ) AS cntry
        WHERE cntry.rown=1
)  twodigitprfx
ON SUBSTR(calledpartyaddress,1,2) = twodigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 1
        ) AS cntry
        WHERE cntry.rown=1
)  onedigitprfx
ON SUBSTR(calledpartyaddress,1,1) = onedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
  SELECT 
    pll.alpha2_cd AS alpha2_cd,
    pll.country_name AS country_name
  FROM
  (
        SELECT 
            DISTINCT TRIM(alpha2_cd) AS alpha2_cd,
            TRIM(country_name) AS country_name,
            ROW_NUMBER() OVER(
                PARTITION BY pl.alpha2_cd ORDER BY pl.effective_date desc
            ) AS rown  
        FROM ela_v21.plmn_settlement_gg pl
        WHERE 
          TRIM(alpha2_cd) IS NOT NULL 
          AND trim(alpha2_cd) <> ''
  ) AS pll
  WHERE pll.rown=1
)  wificountry 
ON regexp_extract(COALESCE(lst3accessnetworkinfo,lst2accessnetworkinfo,lst1accessnetworkinfo),'\;(.*)',1)=wificountry.alpha2_cd
LEFT OUTER JOIN
(
    SELECT 
        npnx.npa npa,
        npnx.nxx nxx,
        npnx.state_code state_code,
        CASE 
            WHEN sc.country_code = 'CAN' 
                THEN 'CANADA'
            WHEN sc.country_code = 'USA'
                THEN 'USA'
            ELSE '?'
        END country_name
    FROM 
        ela_v21.npa_nxx_gg npnx
    LEFT OUTER JOIN
    (
        SELECT state_code,
                country_code
        FROM cdrdm.dim_state_mps
    ) sc
    ON sc.state_code = npnx.state_code
)  npanxx
ON  (SUBSTR(calledpartyaddress,2,3) = npanxx.npa 
    AND SUBSTR(calledpartyaddress,5,3) = npanxx.nxx)
LEFT OUTER JOIN
(
    SELECT 
        DISTINCT spnm.npa npa,
        spnm.nxx nxx,
        'SPECIAL NUMBER' AS specialnumber 
    FROM 
        ela_v21.special_numbers_gg spnm
)  specialnum
ON  (SUBSTR(calledpartyaddress,2,3) = specialnum.npa
    AND SUBSTR(calledpartyaddress,5,3) = specialnum.nxx)
LEFT OUTER JOIN
(
  SELECT
    from_unixtime(CAST (mediaordomainvalue[0] AS BIGINT),'HH:mm:ss') 
      AS CALLSTARTTIME,
    CASE
      WHEN ((lstofnrmlmediac2chgtime IS NULL) AND (lst2changetime IS NULL))
            THEN
                from_unixtime(CAST (unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss') 
                            AS BIGINT),'HH:mm:ss') 
      WHEN ((lstofnrmlmediac2chgtime IS NOT NULL) OR (lst2changetime IS NOT NULL)) 
            THEN
                from_unixtime(CAST (COALESCE(LEAD(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
                            unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) 
                            AS BIGINT),'HH:mm:ss') 
      ELSE '?'
    END
      AS CALLENDTIME,
    CONCAT(IF(roleofnode='1','MTC ','MOC '),mediaordomainvalue[1])
      AS CALLTYPE,
    mediaordomainvalue[1] 
      AS SERVICE,
    CASE
       WHEN ((lstofnrmlmediac2chgtime IS NULL) AND (lst2changetime IS NULL))
            THEN
                CAST(
                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss') - 
                    unix_timestamp(srvcdeliverystarttimestamp,'yyyy-MM-dd HH:mm:ss') 
                    AS BIGINT
                    )
        WHEN ((lstofnrmlmediac2chgtime IS NOT NULL) OR (lst2changetime IS NOT NULL)) 
            THEN
                CAST(COALESCE(lead(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) - mediaordomainvalue[0] AS BIGINT) 
        ELSE '?'
    END
      AS CALLDURATIONSECS,
    CASE
       WHEN ((lstofnrmlmediac2chgtime IS NULL) AND (lst2changetime IS NULL))
            THEN
            CAST(CEIL((
                unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss') - 
                unix_timestamp(srvcdeliverystarttimestamp,'yyyy-MM-dd HH:mm:ss')
                )/60)
                AS BIGINT
                )
        WHEN ((lstofnrmlmediac2chgtime IS NOT NULL) OR (lst2changetime IS NOT NULL)) 
            THEN
                CAST(CEIL((COALESCE(lead(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) - mediaordomainvalue[0])/60) AS BIGINT) 
        ELSE '?'
    END
      AS CALLDURATIONMINS,
      mediaordomainvalue[1] AS type,
      mediaordomainkey,
      mediaordomainvalue[0] AS changestarts,
      COALESCE(lead(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
               unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) 
        AS changeends,
      subscriberno AS splitsubscriberno,
      sipmethod AS splitsipmethod,
      roleofnode AS splitroleofnode,
      srvcrequestdttmnrml_date AS splitsrvcrequestdttmnrml_date,
      srvcdeliverystarttimestamp AS splitsrvcdeliverystarttimestamp,
      srvcdeliveryendtimestamp AS splitsrvcdeliveryendtimestamp,
      file_name AS splitfile_name
  FROM cdrdm.fact_imm_cdr_stg splitimm
  LATERAL VIEW EXPLODE
    (
    map(    
            'lstofnrmlmediac1chgtime',
            ARRAY
            (
                CAST(unix_timestamp(lstofnrmlmediac1chgtime,'yyyy-MM-dd HH:mm:ss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    (
                                        lstofnrmlmediac1desctype1 = '1' OR 
                                        lstofnrmlmediac1desctype2 = '1' OR 
                                        lstofnrmlmediac1desctype3 = '1'
                                    )
                                    AND
                                    (
                                        lst1accessdomain = '2'
                                    )
                                )  
                                    THEN 'ViLTE'
                            WHEN( 
                                    (
                                        lstofnrmlmediac1desctype1 = '1' OR 
                                        lstofnrmlmediac1desctype2 = '1' OR 
                                        lstofnrmlmediac1desctype3 = '1'
                                    )
                                    AND
                                    lst1accessdomain = '20'
                                )
                                    THEN 'ViWiFi'
                            WHEN( 
                                    lstofnrmlmediac1desctype1 = '0'
                                    AND
                                    lst1accessdomain = '2'
                                )
                                THEN 'VoLTE'
                            WHEN( 
                                    lstofnrmlmediac1desctype1 = '0'
                                    AND
                                    lst1accessdomain = '20'
                                )
                                THEN 'VoWiFi'
                            ELSE '?'                        
                        END 
                        AS STRING)
            ),
            'lstofnrmlmediac2chgtime',
            ARRAY
            (
                CAST(unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    ( 
                                        lstofnrmlmediac2desctype1 = '1' OR 
                                        lstofnrmlmediac2desctype2 = '1' OR 
                                        lstofnrmlmediac2desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '2'
                                        )
                                    )
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    ( 
                                        lstofnrmlmediac2desctype1 = '1' OR 
                                        lstofnrmlmediac2desctype2 = '1' OR 
                                        lstofnrmlmediac2desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '20'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '20'
                                        )
                                    )                                
                                )
                                THEN 'ViWiFi'
                            WHEN( 
                                    lstofnrmlmediac2desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '2'
                                        )
                                    )
                                )
                                THEN 'VoLTE'
                            WHEN( 
                                    lstofnrmlmediac2desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '20'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '20'
                                        )
                                    )
                                )
                                THEN 'VoWiFi'
                            ELSE '?'
                        END 
                        AS STRING)
            ),
            'lst2changetime',
            ARRAY
            (
                CAST(unix_timestamp(lst2changetime,'yyyyMMddHHmmss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    ( 
                                        (
                                            ( 
                                                lstofnrmlmediac1desctype1 = '1' OR 
                                                lstofnrmlmediac1desctype2 = '1' OR 
                                                lstofnrmlmediac1desctype3 = '1'
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR 
                                                lstofnrmlmediac2desctype2 = '1' OR 
                                                lstofnrmlmediac2desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '2' 
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    ( 
                                        (
                                            ( 
                                                lstofnrmlmediac1desctype1 = '1' OR 
                                                lstofnrmlmediac1desctype2 = '1' OR 
                                                lstofnrmlmediac1desctype3 = '1'
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR 
                                                lstofnrmlmediac2desctype2 = '1' OR 
                                                lstofnrmlmediac2desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '20' 
                                )
                                THEN 'ViWiFi'
                            WHEN(
                                    ( 
                                        (
                                            lstofnrmlmediac1desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '2' 
                                )
                                THEN 'VoLTE'
                            WHEN(
                                    ( 
                                        (
                                            lstofnrmlmediac1desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '20' 
                                )
                                THEN 'VoWiFi'
                            ELSE '?'
                        END
                        AS STRING)
            ),
            'lstofnrmlmediac3chgtime',
            ARRAY
            (
                CAST(unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    ( 
                                        lstofnrmlmediac3desctype1 = '1' OR 
                                        lstofnrmlmediac3desctype2 = '1' OR 
                                        lstofnrmlmediac3desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                    
                                            )
                                            AND 
                                            lst2accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst3accessdomain = '2'
                                        )
                                    )
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    ( 
                                        lstofnrmlmediac3desctype1 = '1' OR 
                                        lstofnrmlmediac3desctype2 = '1' OR 
                                        lstofnrmlmediac3desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                            COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                    )                                    
                                        )
                                        AND 
                                        lst2accessdomain = '20'
                                    )
                                    OR
                                    (
                                        (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                        )
                                        AND
                                        lst3accessdomain = '20'
                                    )
                                )
                                THEN 'ViWiFi'
                            WHEN( 
                                    lstofnrmlmediac3desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst2accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst3accessdomain = '2'           
                                        )
                                    )
                                )
                                THEN 'VoLTE'
                            WHEN( 
                                    lstofnrmlmediac3desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst2accessdomain = '20'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst3accessdomain = '20'
                                        )                                        
                                    )
                                )
                                THEN 'VoWiFi'
                            ELSE '?'
                        END 
                        AS STRING)
            ),
            'lst3changetime',
            ARRAY
            (
                CAST(unix_timestamp(lst3changetime,'yyyyMMddHHmmss') AS STRING),
                CAST(
                        CASE
                            WHEN(
                                    ( 
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR
                                                lstofnrmlmediac2desctype2 = '1' OR
                                                lstofnrmlmediac2desctype3 = '1'   
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac3desctype1 = '1' OR 
                                                lstofnrmlmediac3desctype2 = '1' OR 
                                                lstofnrmlmediac3desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '2' 
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    (
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR 
                                                lstofnrmlmediac2desctype2 = '1' OR 
                                                lstofnrmlmediac2desctype3 = '1'
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac3desctype1 = '1' OR 
                                                lstofnrmlmediac3desctype2 = '1' OR 
                                                lstofnrmlmediac3desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '20' 
                                )
                                THEN 'ViWiFi'
                            WHEN(
                                    ( 
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac3desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '2'
                                )
                                THEN 'VoLTE'
                            WHEN(
                                    (
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac3desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '20'
                                ) 
                                THEN 'VoWiFi'
                            ELSE '?'
                        END
                        AS STRING)
            )
        )
    ) exp AS mediaordomainkey,mediaordomainvalue
  WHERE
    sipmethod='INVITE'
    AND
    mediaordomainvalue[0] IS NOT NULL
    AND
    (
        (--ViLTE and VoLTE CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='2' AND
                lst1accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst1accessnetworkinfo,1,6) <> '302-72' 
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='2' AND
                lst2accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst2accessnetworkinfo,1,6) <> '302-72'
            )
            OR
            (
                lst3accesstype='1' AND
                lst2accessdomain='2' AND
                lst3accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst3accessnetworkinfo,1,6) <> '302-72'
            )
        )
        OR
        (--ViWiFi and VoWiFi CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='20' AND
                lst1accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst1accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='20' AND
                lst2accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst2accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst3accesstype='1' AND
                lst3accessdomain='20' AND
                lst3accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst3accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
        )
  )
  ORDER BY changestarts ASC
) splitcdr
ON
( 
      srvcrequestdttmnrml_date = splitcdr.splitsrvcrequestdttmnrml_date AND
      subscriberno = splitcdr.splitsubscriberno AND
      sipmethod = splitcdr.splitsipmethod AND
      roleofnode = splitcdr.splitroleofnode AND 
      srvcdeliverystarttimestamp = splitcdr.splitsrvcdeliverystarttimestamp AND
      srvcdeliveryendtimestamp = splitcdr.splitsrvcdeliveryendtimestamp AND
      file_name = splitcdr.splitfile_name
);

INSERT INTO TABLE cdrdm.fact_obroamrpts_cdr 
PARTITION(cday,service_type,call_distance_type)
SELECT
  srvcrequestdttmnrml_date 
    AS CALLDAY,
  DATE_FORMAT(SUBSTR(srvcrequestdttmnrml_date,1,10),'MMMMM') 
    AS CALLMONTH, 
  SUBSTR(srvcdeliverystarttimestamp,12,8) 
    AS CALLSTARTTIME,
  SUBSTR(srvcdeliveryendtimestamp,12,8)
    AS CALLENDTIME,
  CASE 
      WHEN SUBSTR(imsi,0,6) = '302720'
          THEN 'Rogers' 
      WHEN SUBSTR(imsi,0,6) = '302370'  
          THEN 'Fido'
      ELSE imsi 
  END 
    AS BRAND,
  NVL(TRIM(imsi),'?') 
    AS IMSI,
  NVL(TRIM(imei),'?') 
    AS IMEI,
  subscriberno 
    AS MSISDN,
  NVL(pll.plmn_tadig,'')  
    AS VPLMNTADIGCode,
  NVL(pll.country_name,'?') 
    AS VISITEDCOUNTRY,
  NVL(pll.plmn_name,'') 
    AS VPLMNNAME,
    CASE  
        WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
        THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,6),'-',''),'0')
        ELSE 
        REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,7),'-','')
    END
    AS MCCMNC,
  CONCAT(IF(roleofnode='1','MTC ','MOC '),'SMS')
    AS CALLTYPE,
  'SMS' 
    AS SERVICE,    
  CASE
    WHEN
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
      COALESCE(npanxx.country_name,
                specialnum.specialnumber,
                fivedigitprfx.country_name,
                fourdigitprfx.country_name,
                '?')
    ELSE
        COALESCE(threedigitprfx.country_name,
                twodigitprfx.country_name,
                onedigitprfx.country_name,
                '?')
  END 
    AS CALLEDCOUNTRY,
  CASE 
    WHEN 
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
        COALESCE(IF(ISNOTNULL(npanxx.country_name),'1',NULL),
                  IF(ISNOTNULL(specialnum.specialnumber),'1',NULL),
                  fivedigitprfx.plmn_country_code,
                  fourdigitprfx.plmn_country_code,
                  '?')
    ELSE 
        COALESCE(threedigitprfx.plmn_country_code,
                twodigitprfx.plmn_country_code,
                onedigitprfx.plmn_country_code,
                '?')
  END 
    AS COUNTRYCODE,    
  CAST(calledpartyaddress AS BIGINT) 
    AS CALLEDNUMBER,
  requestedpartyaddress 
    AS DIALEDNUMBER,
  '' 
    AS CALLDURATIONSECS,
  '' 
    AS CALLDURATIONMINS,
   '' 
    AS DATAVOLUME,
   '' 
    AS CALL_TYPE_LEVEL_2,
   '' 
    AS ACCESS_POINT_NAME_NI,
   '1' 
    AS NUMBEROFEVENTS,
    CASE
        WHEN 
        (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-','') = "" 
            OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
        WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-',''),16,10)
        ELSE
        CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-',''),16,10)
    END
    AS SERVINGCELLID,
    CASE
        WHEN (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-','') = "" 
            OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
        WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-',''),16,10)
        ELSE
        CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-',''),16,10)
    END
    AS SERVINGLAC,
  CURRENT_DATE 
    AS INSERTDATE,
  CURRENT_TIMESTAMP 
    AS INSERTTIME,
  file_name 
    AS FILE_NAME,
  record_type 
    AS RECORD_TYPE,
  family_name 
    AS FAMILY_NAME,
  version_id 
    AS VERSION_ID,
  file_time 
    AS FILE_TIME,
  file_id 
    AS FILE_ID,
  switch_name 
    AS SWITCH_NAME,
CASE
WHEN factcdr.mmtelsplmtrysrvcinfo1id='9444' or
factcdr.mmtelsplmtrysrvcinfo2id='9444' or
factcdr.mmtelsplmtrysrvcinfo3id='9444' or
factcdr.mmtelsplmtrysrvcinfo4id='9444' or
factcdr.mmtelsplmtrysrvcinfo5id='9444' or
factcdr.mmtelsplmtrysrvcinfo6id='9444' or
factcdr.mmtelsplmtrysrvcinfo7id='9444' or
factcdr.mmtelsplmtrysrvcinfo8id='9444' or
factcdr.mmtelsplmtrysrvcinfo9id='9444' or
factcdr.mmtelsplmtrysrvcinfo10id='9444' 
THEN 'Y'
ELSE NULL
END AS CALLCORRECTED,
CAST(REGEXP_REPLACE(SUBSTR(srvcrequestdttmnrml_date,1,10),'-','') AS INT) AS CDAY,
'4G' AS SERVICE_TYPE,
'ROAMING' AS CALL_DISTANCE_TYPE
FROM 
(
        SELECT
        srvcrequestdttmnrml_date,
        srvcdeliverystarttimestamp,
        srvcdeliveryendtimestamp,
        chargeableduration,
        imsi,
        imei,
        subscriberno,
        lst1accesstype,
        lst1accessnetworkinfo,
        lst1accessdomain,
        roleofnode,
        sipmethod,
        calledpartyaddress,
        requestedpartyaddress,
        file_name,
        record_type,
        family_name,
        version_id,
        file_time,
        file_id,
        switch_name,
        MMTelSplmtrySrvcInfo1ID,
MMTelSplmtrySrvcInfo2ID,
if(MMTelInfoSplmtrySrvcInfo3 is not NULL and length(MMTelInfoSplmtrySrvcInfo3) > 0,split(MMTelInfoSplmtrySrvcInfo3,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo3ID,
if(MMTelInfoSplmtrySrvcInfo4 is not NULL and length(MMTelInfoSplmtrySrvcInfo4) > 0,split(MMTelInfoSplmtrySrvcInfo4,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo4ID,
if(MMTelInfoSplmtrySrvcInfo5 is not NULL and length(MMTelInfoSplmtrySrvcInfo5) > 0,split(MMTelInfoSplmtrySrvcInfo5,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo5ID,
if(MMTelInfoSplmtrySrvcInfo6 is not NULL and length(MMTelInfoSplmtrySrvcInfo6) > 0,split(MMTelInfoSplmtrySrvcInfo6,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo6ID,
if(MMTelInfoSplmtrySrvcInfo7 is not NULL and length(MMTelInfoSplmtrySrvcInfo7) > 0,split(MMTelInfoSplmtrySrvcInfo7,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo7ID,
if(MMTelInfoSplmtrySrvcInfo8 is not NULL and length(MMTelInfoSplmtrySrvcInfo8) > 0,split(MMTelInfoSplmtrySrvcInfo8,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo8ID,
if(MMTelInfoSplmtrySrvcInfo9 is not NULL and length(MMTelInfoSplmtrySrvcInfo9) > 0,split(MMTelInfoSplmtrySrvcInfo9,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo9ID,
if(MMTelInfoSplmtrySrvcInfo10 is not NULL and length(MMTelInfoSplmtrySrvcInfo10) > 0,split(MMTelInfoSplmtrySrvcInfo10,'\\|')[0],NULL) as MMTelSplmtrySrvcInfo10ID
  FROM cdrdm.fact_imm_cdr_stg
  WHERE
    sipmethod='MESSAGE'
    AND
    lst1accessdomain = '2' -- Include messages in LTE zone only
    AND
    SUBSTR(lst1accessnetworkinfo,1,6) <> '302-72' -- Exclude Rogers
    AND
    regexp_extract(lst1accessnetworkinfo,'\;(.*)',1) <> 'CA' -- Exclude CANADA roamers
    AND
    lst1accessnetworkinfo <> '000-000-0000-0' -- Exclude unknown MCC-MNC-LAC-CellID
    AND
    lst1accessnetworkinfo <> '---' -- Exclude unknown locations
) factcdr
LEFT OUTER JOIN 
(
    SELECT * FROM 
    (
            SELECT 
                PL.*,
                ROW_NUMBER() OVER(PARTITION BY pl.plmn_code ORDER BY pl.effective_date desc) AS rown  
            FROM 
                ela_v21.plmn_settlement_gg pl
    ) AS pll 
    WHERE pll.rown=1
)  pll 
ON IF ( 
        LENGTH(REGEXP_REPLACE(REGEXP_EXTRACT(lst1accessnetworkinfo,'(.+?-.+?)-.+?'),'-',''))=6,
        REGEXP_REPLACE(REGEXP_EXTRACT(lst1accessnetworkinfo,'(.+?-.+?)-.+?'),'-',''),
        CONCAT(REGEXP_REPLACE(REGEXP_EXTRACT(lst1accessnetworkinfo,'(.+?-.+?)-.+?'),'-',''),'0')
 )=pll.plmn_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
              WHERE LENGTH(ps.plmn_country_code) = 5
        ) AS cntry
        WHERE cntry.rown=1
) fivedigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fivedigitprfx.plmn_country_code
LEFT OUTER JOIN
  (
      SELECT
          cntry.plmn_country_code,
          cntry.country_name
      FROM
          (
              SELECT
                  DISTINCT ps.plmn_country_code,
                  ps.country_name,
                  row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
              FROM
                  ela_v21.plmn_settlement_gg ps
                WHERE LENGTH(ps.plmn_country_code) = 4
          ) AS cntry
          WHERE cntry.rown=1
  ) fourdigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fourdigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE ps.plmn_country_code <> '1'
                AND LENGTH(ps.plmn_country_code) = 3
        ) AS cntry
        WHERE cntry.rown=1
) threedigitprfx
ON SUBSTR(calledpartyaddress,1,3) = threedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 2
        ) AS cntry
        WHERE cntry.rown=1
)  twodigitprfx
ON SUBSTR(calledpartyaddress,1,2) = twodigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 1
        ) AS cntry
        WHERE cntry.rown=1
)  onedigitprfx
ON SUBSTR(calledpartyaddress,1,1) = onedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT 
        npnx.npa npa,
        npnx.nxx nxx,
        npnx.state_code state_code,
        CASE 
            WHEN sc.country_code = 'CAN' 
                THEN 'CANADA'
            WHEN sc.country_code = 'USA'
                THEN 'USA'
            ELSE '?'
        END country_name
    FROM 
        ela_v21.npa_nxx_gg npnx
    LEFT OUTER JOIN
    (
        SELECT state_code,
                country_code
        FROM cdrdm.dim_state_mps
    ) sc
    ON sc.state_code = npnx.state_code
)  npanxx
ON  (SUBSTR(calledpartyaddress,2,3) = npanxx.npa 
    AND SUBSTR(calledpartyaddress,5,3) = npanxx.nxx)
LEFT OUTER JOIN
(
    SELECT 
        DISTINCT spnm.npa npa,
        spnm.nxx nxx,
        'SPECIAL NUMBER' AS specialnumber 
    FROM 
        ela_v21.special_numbers_gg spnm
)  specialnum
ON  (SUBSTR(calledpartyaddress,2,3) = specialnum.npa
    AND SUBSTR(calledpartyaddress,5,3) = specialnum.nxx);

drop table cdrdm.LES_IMM_INFO_STEP1;
truncate table cdrdm.FACT_IMM_CDR_STG;
