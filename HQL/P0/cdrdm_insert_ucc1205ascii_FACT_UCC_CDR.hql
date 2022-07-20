--[Version History]
--0.1 - danchang - 4/06/2016 - Latest version from Alfaz,ext_ela_v21 test tables
--e.g. ELA_V21 -> EXT_ELA_V21
--0.2 - danchang - 4/22/2016 - Updates to transform logic for fields: 
--MMTelSplmtrySrvcInfo1Redir, MMTelSplmtrySrvcInfo2Redir, MMTelSplmtrySrvcInfo3Redir, MMTelSplmtrySrvcInfo4Redir, MMTelSplmtrySrvcInfo5Redir
--SubscriberE164, OtherPartyAddress, SubscriberNo, RequestedPartyAddress
--0.3 - danchang - 4/26/2016 - Fixed mapping for field reasonCodeEstablishedSession
--Saseenthar - 06/07/2016 - Included new fields TR_PRODUCT_TYPE, WL_*, *_ORIG, networkCallType, carrierIdCode, End_User_SubsID_CTN, End_User_SubsID_Dom_fl, End_User_SubsID_Dom_ty as part of Phase 2 UCC requirement
--Modified transform logic for fields:
--MMTelSplmtrySrvcInfo1ID, MMTelSplmtrySrvcInfo1Act, MMTelSplmtrySrvcInfo1Redir,
--MMTelSplmtrySrvcInfo2ID, MMTelSplmtrySrvcInfo2Act, MMTelSplmtrySrvcInfo2Redir,
--MMTelSplmtrySrvcInfo3ID, MMTelSplmtrySrvcInfo3Act, MMTelSplmtrySrvcInfo3Redir,
--MMTelSplmtrySrvcInfo4ID, MMTelSplmtrySrvcInfo4Act, MMTelSplmtrySrvcInfo4Redir,
--MMTelSplmtrySrvcInfo5ID, MMTelSplmtrySrvcInfo5Act, MMTelSplmtrySrvcInfo5Redir
--0.5 - fanshelm - 07/25/2016 - Update NULL condition -> if field doesn't exist then populate NULL (don't leave it blank)
--0.6 - humane - 08/10/2016 - Modified transform logic for field 'End_User_SubsID_CTN' to match STM
--0.7 - humane - 08/24/2016 - Modified transform logic for field MMTelSplmtrySrvcInfo1Redir,MMTelSplmtrySrvcInfo2Redir,MMTelSplmtrySrvcInfo3Redir,MMTelSplmtrySrvcInfo4Redir,MMTelSplmtrySrvcInfo5Redir to match STM

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
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
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;

SET WHERE_clause;

-- Dropping Temp tables if there are any from last run...
DROP TABLE ext_cdrdm.ucc1205ascii_temp1;
DROP TABLE ext_cdrdm.ucc1205ascii_temp2;
DROP TABLE ext_cdrdm.ucc1205ascii_BAN;
DROP TABLE ext_cdrdm.tmp_SUB_ALS_IND_tbl;
DROP TABLE ext_cdrdm.tmp_SUB_OTH_PARTY_ALS_IND_tbl;
DROP TABLE ext_cdrdm.tmp_TR_RE_DIR1_ALS_IND_tbl;
DROP TABLE ext_cdrdm.tmp_TR_SUB_IND_GEN_tbl;

--  PHASE 2 CHANGES - STARTS
DROP TABLE ext_cdrdm.ucc1205ascii_WL_BAN;
DROP TABLE ext_cdrdm.tmp_WL_SUB_ALS_IND_tbl;
DROP TABLE ext_cdrdm.tmp_WL_SUB_OTH_PARTY_ALS_IND_tbl;
DROP TABLE ext_cdrdm.tmp_WL_RE_DIR1_ALS_IND_tbl;
-- PHASE 2 CHANGES - ENDS

CREATE TABLE ext_cdrdm.ucc1205ascii_temp1 AS
SELECT
trim(recordType) AS RecordType,
trim(retransmission) AS Retransmission,
trim(sIP_Method) AS SIPMethod,
trim(role_of_Node) AS RoleOfNode,
trim(nodeAddress) AS NodeAddress,
trim(session_Id) AS SessionId,
trim(calling_Party_Address) as CallingPartyAddress_orig,
																																									
if((calling_Party_Address is not NULL) AND length(calling_Party_Address) > 0, if(INSTR(UPPER(calling_Party_Address),'SIP:+') > 0 OR INSTR(UPPER(calling_Party_Address),'TEL:+') > 0, if(instr(trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,'+') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,'+') + 1),'@') - 1)) , '.') = 0 or instr( trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,'+') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,'+') + 1),'@') - 1)), '-') = 0 or instr( trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,'+') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,'+') + 1),'@') - 1)), '+') = 0, cast(trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,'+') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,'+') + 1),'@') - 1)) as bigint), NULL),if(INSTR(UPPER(calling_Party_Address),'SIP:') > 0 or INSTR(UPPER(calling_Party_Address),'TEL:') > 0, if(instr(trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,':') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,':') + 1),'@') - 1)) , '.') = 0 or instr( trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,':') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,':') + 1),'@') - 1)), '-') = 0 or instr( trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,':') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,':') + 1),'@') - 1)), '+') = 0, cast(trim(SUBSTR(calling_Party_Address,INSTR(calling_Party_Address,':') + 1, INSTR(SUBSTR(calling_Party_Address, INSTR(calling_Party_Address,':') + 1),'@') - 1)) as bigint), NULL),if(substring(calling_Party_Address, 1, 1)='+',substr(calling_Party_Address,2),cast(trim(calling_Party_Address) as bigint)))),NULL) as CallingPartyAddress,
trim(called_Party_Address) as CalledPartyAddress_orig,																																									
if((called_Party_Address is not NULL) AND length(called_Party_Address) > 0, if(INSTR(UPPER(called_Party_Address),'SIP:+') > 0 OR INSTR(UPPER(called_Party_Address),'TEL:+') > 0, if(instr(trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,'+') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,'+') + 1),'@') - 1)) , '.') = 0 or instr( trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,'+') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,'+') + 1),'@') - 1)), '-') = 0 or instr( trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,'+') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,'+') + 1),'@') - 1)), '+') = 0, cast(trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,'+') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,'+') + 1),'@') - 1)) as bigint), NULL),if(INSTR(UPPER(called_Party_Address),'SIP:') > 0 or INSTR(UPPER(called_Party_Address),'TEL:') > 0, if(instr(trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,':') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,':') + 1),'@') - 1)) , '.') = 0 or instr( trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,':') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,':') + 1),'@') - 1)), '-') = 0 or instr( trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,':') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,':') + 1),'@') - 1)), '+') = 0, cast(trim(SUBSTR(called_Party_Address,INSTR(called_Party_Address,':') + 1, INSTR(SUBSTR(called_Party_Address, INSTR(called_Party_Address,':') + 1),'@') - 1)) as bigint), NULL),if(substring(called_Party_Address, 1, 1)='+',substr(called_Party_Address,2),cast(trim(called_Party_Address) as bigint)))),NULL) as CalledPartyAddress,
if((serviceRequestTimeStamp is not NULL) and length(cast(serviceRequestTimeStamp as bigint)) > 0,CONCAT(SUBSTR(cast(serviceRequestTimeStamp as bigint),1,4),'-',SUBSTR(cast(serviceRequestTimeStamp as bigint),5,2),'-',SUBSTR(cast(serviceRequestTimeStamp as bigint),7,2),' ',SUBSTR(cast(serviceRequestTimeStamp as bigint),9,2),':',SUBSTR(cast(serviceRequestTimeStamp as bigint),11,2),':',SUBSTR(cast(serviceRequestTimeStamp as bigint),13,2)), NULL) as SrvcRequestTimeStamp,
if((serviceDeliveryStartTimeStamp is not NULL) and length(cast(serviceDeliveryStartTimeStamp as bigint)) > 0,CONCAT(SUBSTR(cast(serviceDeliveryStartTimeStamp as bigint),1,4),'-',SUBSTR(cast(serviceDeliveryStartTimeStamp as bigint),5,2),'-',SUBSTR(cast(serviceDeliveryStartTimeStamp as bigint),7,2),' ',SUBSTR(cast(serviceDeliveryStartTimeStamp as bigint),9,2),':',SUBSTR(cast(serviceDeliveryStartTimeStamp as bigint),11,2),':',SUBSTR(cast(serviceDeliveryStartTimeStamp as bigint),13,2)), NULL) as SrvcDeliveryStartTimeStamp,
if((serviceDeliveryEndTimeStamp is not NULL) and length(cast(serviceDeliveryEndTimeStamp as bigint)) > 0,CONCAT(SUBSTR(cast(serviceDeliveryEndTimeStamp as bigint),1,4),'-',SUBSTR(cast(serviceDeliveryEndTimeStamp as bigint),5,2),'-',SUBSTR(cast(serviceDeliveryEndTimeStamp as bigint),7,2),' ',SUBSTR(cast(serviceDeliveryEndTimeStamp as bigint),9,2),':',SUBSTR(cast(serviceDeliveryEndTimeStamp as bigint),11,2),':',SUBSTR(cast(serviceDeliveryEndTimeStamp as bigint),13,2)), NULL) as SrvcDeliveryEndTimeStamp,
if((RecordOpeningTime is not NULL) and length(cast(RecordOpeningTime as bigint)) > 0,CONCAT(SUBSTR(cast(RecordOpeningTime as bigint),1,4),'-',SUBSTR(cast(RecordOpeningTime as bigint),5,2),'-',SUBSTR(cast(RecordOpeningTime as bigint),7,2),' ',SUBSTR(cast(RecordOpeningTime as bigint),9,2),':',SUBSTR(cast(RecordOpeningTime as bigint),11,2),':',SUBSTR(cast(RecordOpeningTime as bigint),13,2)), NULL) as RecordOpeningTime,
if((RecordClosureTime is not NULL) and length(cast(RecordClosureTime as bigint)) > 0,CONCAT(SUBSTR(cast(RecordClosureTime as bigint),1,4),'-',SUBSTR(cast(RecordClosureTime as bigint),5,2),'-',SUBSTR(cast(RecordClosureTime as bigint),7,2),' ',SUBSTR(cast(RecordClosureTime as bigint),9,2),':',SUBSTR(cast(RecordClosureTime as bigint),11,2),':',SUBSTR(cast(RecordClosureTime as bigint),13,2)), NULL) as RecordClosureTime,
if((serviceRequestTimeStampNormalized is not NULL) and length(cast(serviceRequestTimeStampNormalized as bigint)) > 0,CONCAT(SUBSTR(cast(serviceRequestTimeStampNormalized as bigint),1,4),'-',SUBSTR(cast(serviceRequestTimeStampNormalized as bigint),5,2),'-',SUBSTR(cast(serviceRequestTimeStampNormalized as bigint),7,2),' ',SUBSTR(cast(serviceRequestTimeStampNormalized as bigint),9,2),':',SUBSTR(cast(serviceRequestTimeStampNormalized as bigint),11,2),':',SUBSTR(cast(serviceRequestTimeStampNormalized as bigint),13,2)), NULL) as SrvcRequestDttmNrml,
if((serviceDeliveryStartTimeStampNormalized is not NULL) and length(cast(serviceDeliveryStartTimeStampNormalized as bigint)) > 0,CONCAT(SUBSTR(cast(serviceDeliveryStartTimeStampNormalized as bigint),1,4),'-',SUBSTR(cast(serviceDeliveryStartTimeStampNormalized as bigint),5,2),'-',SUBSTR(cast(serviceDeliveryStartTimeStampNormalized as bigint),7,2),' ',SUBSTR(cast(serviceDeliveryStartTimeStampNormalized as bigint),9,2),':',SUBSTR(cast(serviceDeliveryStartTimeStampNormalized as bigint),11,2),':',SUBSTR(cast(serviceDeliveryStartTimeStampNormalized as bigint),13,2)), NULL) as SrvcDeliveryStartDttmNrml,
if((serviceDeliveryEndTimeStampNormalized is not NULL) and length(cast(serviceDeliveryEndTimeStampNormalized as bigint)) > 0,CONCAT(SUBSTR(cast(serviceDeliveryEndTimeStampNormalized as bigint),1,4),'-',SUBSTR(cast(serviceDeliveryEndTimeStampNormalized as bigint),5,2),'-',SUBSTR(cast(serviceDeliveryEndTimeStampNormalized as bigint),7,2),' ',SUBSTR(cast(serviceDeliveryEndTimeStampNormalized as bigint),9,2),':',SUBSTR(cast(serviceDeliveryEndTimeStampNormalized as bigint),11,2),':',SUBSTR(cast(serviceDeliveryEndTimeStampNormalized as bigint),13,2)), NULL) as SrvcDeliveryEndDttmNrml,
cast(trim(ChargeableDuration) as bigint) AS ChargeableDuration,
cast(trim(timeFromSipRequestToStartOfCharging) as bigint) AS TmFromSipRqstStartOfChrgng,
trim(InterOperatorIdentifiers) AS InterOperatorIdentifiers,
trim(LocalRecordSequenceNumber) AS LocalRecordSequenceNumber,
if(PartialOutputRecordNumber is not NULL and length(PartialOutputRecordNumber) > 0,trim(PartialOutputRecordNumber),NULL) AS PartialOutputRecordNumber,
trim(CauseForRecordClosing) as CauseForRecordClosing_Orig,
if(CauseForRecordClosing is not NULL and length(CauseForRecordClosing) > 0,if(split(CauseForRecordClosing,'\\|')[0] is not null and length(split(CauseForRecordClosing,'\\|')[0]) > 0,split(CauseForRecordClosing,'\\|')[0],NULL),NULL) as causeCodePreEstablishedSession,
if(CauseForRecordClosing is not NULL and length(CauseForRecordClosing) > 0,if(split(CauseForRecordClosing,'\\|')[1] is not null and length(split(CauseForRecordClosing,'\\|')[1]) > 0,split(CauseForRecordClosing,'\\|')[1],NULL),NULL) as reasonCodePreEstablishedSession,
if(CauseForRecordClosing is not NULL and length(CauseForRecordClosing) > 0,if(split(CauseForRecordClosing,'\\|')[2] is not null and length(split(CauseForRecordClosing,'\\|')[2]) > 0,split(CauseForRecordClosing,'\\|')[2],NULL),NULL) as causeCodeEstablishedSession,
if(CauseForRecordClosing is not NULL and length(CauseForRecordClosing) > 0,if(split(CauseForRecordClosing,'\\|')[3] is not null and length(split(CauseForRecordClosing,'\\|')[3]) > 0,split(CauseForRecordClosing,'\\|')[3],NULL),NULL) as reasonCodeEstablishedSession,
trim(incomplete_CDR_Indication) as incomplete_CDR_Ind_orig,
if(incomplete_CDR_Indication is not NULL and length(incomplete_CDR_Indication) > 0,if(split(incomplete_CDR_Indication,'\\|')[0] is not null and length(split(incomplete_CDR_Indication,'\\|')[0]) > 0,split(incomplete_CDR_Indication,'\\|')[0],NULL),NULL) as ACRStartLost,
if(incomplete_CDR_Indication is not NULL and length(incomplete_CDR_Indication) > 0,if(split(incomplete_CDR_Indication,'\\|')[1] is not null and length(split(incomplete_CDR_Indication,'\\|')[1]) > 0,split(incomplete_CDR_Indication,'\\|')[1],NULL),NULL) as ACRInterimLost,
if(incomplete_CDR_Indication is not NULL and length(incomplete_CDR_Indication) > 0,if(split(incomplete_CDR_Indication,'\\|')[2] is not null and length(split(incomplete_CDR_Indication,'\\|')[2]) > 0,split(incomplete_CDR_Indication,'\\|')[2],NULL),NULL) as ACRStopLost,
if(incomplete_CDR_Indication is not NULL and length(incomplete_CDR_Indication) > 0,if(split(incomplete_CDR_Indication,'\\|')[3] is not null and length(split(incomplete_CDR_Indication,'\\|')[3]) > 0,split(incomplete_CDR_Indication,'\\|')[3],NULL),NULL) as IMSILost,
if(incomplete_CDR_Indication is not NULL and length(incomplete_CDR_Indication) > 0,if(split(incomplete_CDR_Indication,'\\|')[4] is not null and length(split(incomplete_CDR_Indication,'\\|')[4]) > 0,split(incomplete_CDR_Indication,'\\|')[4],NULL),NULL) as ACRSCCASStartLost,
if(incomplete_CDR_Indication is not NULL and length(incomplete_CDR_Indication) > 0,if(split(incomplete_CDR_Indication,'\\|')[5] is not null and length(split(incomplete_CDR_Indication,'\\|')[5]) > 0,split(incomplete_CDR_Indication,'\\|')[5],NULL),NULL) as ACRSCCASInterimLost,
if(incomplete_CDR_Indication is not NULL and length(incomplete_CDR_Indication) > 0,if(split(incomplete_CDR_Indication,'\\|')[6] is not null and length(split(incomplete_CDR_Indication,'\\|')[6]) > 0,split(incomplete_CDR_Indication,'\\|')[6],NULL),NULL) as ACRSCCASStopLost,
trim(iMS_Charging_Identifier) AS IMSChargingIdentifier,
trim(list_Of_SDP_Media_Components) AS LstOfSDPMediaComponents,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[0] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[0] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[0] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[0] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[0] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[0] as bigint),13,2)),NULL) as LstOfNrmlMediaC1ChgTime,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[1] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[1] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[1] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[1] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[1] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components1,'\\|')[1] as bigint),13,2)),NULL) as LstOfNrmlMediaC1ChgTimeNor,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[2] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[2]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[2],NULL),NULL) as LstOfNrmlMediaC1DescType1,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[3] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[3]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[3],NULL),NULL) as LstOfNrmlMediaC1DescCodec1,
cast(trim(if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[4] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[4]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[4],NULL),NULL)) as int) as LstOfNrmlMediaC1DescBndW1,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[5] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[5]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[5],NULL),NULL) as LstOfNrmlMediaC1DescType2,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[6] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[6]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[6],NULL),NULL) as LstOfNrmlMediaC1DescCodec2,
cast(trim(if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[7] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[7]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[7],NULL),NULL)) as int) as LstOfNrmlMediaC1DescBndW2,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[8] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[8]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[8],NULL),NULL) as LstOfNrmlMediaC1DescType3,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[9] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[9]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[9],NULL),NULL) as LstOfNrmlMediaC1DescCodec3,
cast(trim(if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[10] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[10]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[10],NULL),NULL)) as int) as LstOfNrmlMediaC1DescBndW3,
if(list_Of_Normalized_Media_Components1 is not NULL and length(list_Of_Normalized_Media_Components1) > 0,if(split(list_Of_Normalized_Media_Components1,'\\|')[11] is not null and length(split(list_Of_Normalized_Media_Components1,'\\|')[11]) > 0,split(list_Of_Normalized_Media_Components1,'\\|')[11],NULL),NULL) as LstOfNrmlMediaC1medInitFlg,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[0] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[0] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[0] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[0] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[0] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[0] as bigint),13,2)),NULL) as LstOfNrmlMediaC2ChgTime,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[1] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[1] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[1] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[1] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[1] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components2,'\\|')[1] as bigint),13,2)),NULL) as LstOfNrmlMediaC2ChgTimeNor,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[2] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[2]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[2],NULL),NULL) as LstOfNrmlMediaC2DescType1,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[3] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[3]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[3],NULL),NULL) as LstOfNrmlMediaC2DescCodec1,
cast(trim(if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[4] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[4]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[4],NULL),NULL)) as int) as LstOfNrmlMediaC2DescBndW1,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[5] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[5]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[5],NULL),NULL) as LstOfNrmlMediaC2DescType2,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[6] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[6]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[6],NULL),NULL) as LstOfNrmlMediaC2DescCodec2,
cast(trim(if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[7] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[7]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[7],NULL),NULL)) as int) as LstOfNrmlMediaC2DescBndW2,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[8] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[8]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[8],NULL),NULL) as LstOfNrmlMediaC2DescType3,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[9] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[9]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[9],NULL),NULL) as LstOfNrmlMediaC2DescCodec3,
cast(trim(if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[10] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[10]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[10],NULL),NULL)) as int) as LstOfNrmlMediaC2DescBndW3,
if(list_Of_Normalized_Media_Components2 is not NULL and length(list_Of_Normalized_Media_Components2) > 0,if(split(list_Of_Normalized_Media_Components2,'\\|')[11] is not null and length(split(list_Of_Normalized_Media_Components2,'\\|')[11]) > 0,split(list_Of_Normalized_Media_Components2,'\\|')[11],NULL),NULL) as LstOfNrmlMediaC2medInitFlg,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[0] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[0] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[0] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[0] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[0] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[0] as bigint),13,2)),NULL) as LstOfNrmlMediaC3ChgTime,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[1] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[1] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[1] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[1] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[1] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components3,'\\|')[1] as bigint),13,2)),NULL) as LstOfNrmlMediaC3ChgTimeNor,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[2] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[2]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[2],NULL),NULL) as LstOfNrmlMediaC3DescType1,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[3] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[3]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[3],NULL),NULL) as LstOfNrmlMediaC3DescCodec1,
cast(trim(if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[4] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[4]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[4],NULL),NULL)) as int) as LstOfNrmlMediaC3DescBndW1,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[5] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[5]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[5],NULL),NULL) as LstOfNrmlMediaC3DescType2,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[6] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[6]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[6],NULL),NULL) as LstOfNrmlMediaC3DescCodec2,
cast(trim(if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[7] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[7]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[7],NULL),NULL)) as int) as LstOfNrmlMediaC3DescBndW2,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[8] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[8]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[8],NULL),NULL) as LstOfNrmlMediaC3DescType3,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[9] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[9]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[9],NULL),NULL) as LstOfNrmlMediaC3DescCodec3,
cast(trim(if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[10] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[10]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[10],NULL),NULL)) as int) as LstOfNrmlMediaC3DescBndW3,
if(list_Of_Normalized_Media_Components3 is not NULL and length(list_Of_Normalized_Media_Components3) > 0,if(split(list_Of_Normalized_Media_Components3,'\\|')[11] is not null and length(split(list_Of_Normalized_Media_Components3,'\\|')[11]) > 0,split(list_Of_Normalized_Media_Components3,'\\|')[11],NULL),NULL) as LstOfNrmlMediaC3medInitFlg,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[0] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[0] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[0] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[0] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[0] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[0] as bigint),13,2)),NULL) as LstOfNrmlMediaC4ChgTime,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[1] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[1] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[1] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[1] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[1] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components4,'\\|')[1] as bigint),13,2)),NULL) as LstOfNrmlMediaC4ChgTimeNor,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[2] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[2]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[2],NULL),NULL) as LstOfNrmlMediaC4DescType1,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[3] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[3]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[3],NULL),NULL) as LstOfNrmlMediaC4DescCodec1,
cast(trim(if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[4] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[4]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[4],NULL),NULL)) as int) as LstOfNrmlMediaC4DescBndW1,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[5] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[5]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[5],NULL),NULL) as LstOfNrmlMediaC4DescType2,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[6] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[6]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[6],NULL),NULL) as LstOfNrmlMediaC4DescCodec2,
cast(trim(if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[7] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[7]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[7],NULL),NULL)) as int) as LstOfNrmlMediaC4DescBndW2,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[8] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[8]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[8],NULL),NULL) as LstOfNrmlMediaC4DescType3,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[9] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[9]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[9],NULL),NULL) as LstOfNrmlMediaC4DescCodec3,
cast(trim(if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[10] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[10]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[10],NULL),NULL)) as int) as LstOfNrmlMediaC4DescBndW3,
if(list_Of_Normalized_Media_Components4 is not NULL and length(list_Of_Normalized_Media_Components4) > 0,if(split(list_Of_Normalized_Media_Components4,'\\|')[11] is not null and length(split(list_Of_Normalized_Media_Components4,'\\|')[11]) > 0,split(list_Of_Normalized_Media_Components4,'\\|')[11],NULL),NULL) as LstOfNrmlMediaC4medInitFlg,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[0] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[0] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[0] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[0] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[0] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[0] as bigint),13,2)),NULL) as LstOfNrmlMediaC5ChgTime,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,CONCAT(SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[1] as bigint),1,4),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[1] as bigint),5,2),'-',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[1] as bigint),7,2),' ',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[1] as bigint),9,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[1] as bigint),11,2),':',SUBSTR(cast(split(list_Of_Normalized_Media_Components5,'\\|')[1] as bigint),13,2)),NULL) as LstOfNrmlMediaC5ChgTimeNor,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[2] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[2]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[2],NULL),NULL) as LstOfNrmlMediaC5DescType1,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[3] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[3]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[3],NULL),NULL) as LstOfNrmlMediaC5DescCodec1,
cast(trim(if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[4] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[4]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[4],NULL),NULL)) as int) as LstOfNrmlMediaC5DescBndW1,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[5] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[5]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[5],NULL),NULL) as LstOfNrmlMediaC5DescType2,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[6] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[6]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[6],NULL),NULL) as LstOfNrmlMediaC5DescCodec2,
cast(trim(if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[7] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[7]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[7],NULL),NULL)) as int) as LstOfNrmlMediaC5DescBndW2,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[8] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[8]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[8],NULL),NULL) as LstOfNrmlMediaC5DescType3,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[9] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[9]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[9],NULL),NULL) as LstOfNrmlMediaC5DescCodec3,
cast(trim(if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[10] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[10]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[10],NULL),NULL)) as int) as LstOfNrmlMediaC5DescBndW3,
if(list_Of_Normalized_Media_Components5 is not NULL and length(list_Of_Normalized_Media_Components5) > 0,if(split(list_Of_Normalized_Media_Components5,'\\|')[11] is not null and length(split(list_Of_Normalized_Media_Components5,'\\|')[11]) > 0,split(list_Of_Normalized_Media_Components5,'\\|')[11],NULL),NULL) as LstOfNrmlMediaC5medInitFlg,
trim(list_Of_Normalized_Media_Components6) AS list_Of_Normalized_Media_Components6,
trim(list_Of_Normalized_Media_Components7) AS list_Of_Normalized_Media_Components7,
trim(list_Of_Normalized_Media_Components8) AS list_Of_Normalized_Media_Components8,
trim(list_Of_Normalized_Media_Components9) AS list_Of_Normalized_Media_Components9,
trim(list_Of_Normalized_Media_Components10) AS list_Of_Normalized_Media_Components10,
trim(list_Of_Normalized_Media_Components11) AS list_Of_Normalized_Media_Components11,
trim(list_Of_Normalized_Media_Components12) AS list_Of_Normalized_Media_Components12,
trim(list_Of_Normalized_Media_Components13) AS list_Of_Normalized_Media_Components13,
trim(list_Of_Normalized_Media_Components14) AS list_Of_Normalized_Media_Components14,
trim(list_Of_Normalized_Media_Components15) AS list_Of_Normalized_Media_Components15,
trim(list_Of_Normalized_Media_Components16) AS list_Of_Normalized_Media_Components16,
trim(list_Of_Normalized_Media_Components17) AS list_Of_Normalized_Media_Components17,
trim(list_Of_Normalized_Media_Components18) AS list_Of_Normalized_Media_Components18,
trim(list_Of_Normalized_Media_Components19) AS list_Of_Normalized_Media_Components19,
trim(list_Of_Normalized_Media_Components20) AS list_Of_Normalized_Media_Components20,
trim(list_Of_Normalized_Media_Components21) AS list_Of_Normalized_Media_Components21,
trim(list_Of_Normalized_Media_Components22) AS list_Of_Normalized_Media_Components22,
trim(list_Of_Normalized_Media_Components23) AS list_Of_Normalized_Media_Components23,
trim(list_Of_Normalized_Media_Components24) AS list_Of_Normalized_Media_Components24,
trim(list_Of_Normalized_Media_Components25) AS list_Of_Normalized_Media_Components25,
trim(list_Of_Normalized_Media_Components26) AS list_Of_Normalized_Media_Components26,
trim(list_Of_Normalized_Media_Components27) AS list_Of_Normalized_Media_Components27,
trim(list_Of_Normalized_Media_Components28) AS list_Of_Normalized_Media_Components28,
trim(list_Of_Normalized_Media_Components29) AS list_Of_Normalized_Media_Components29,
trim(list_Of_Normalized_Media_Components30) AS list_Of_Normalized_Media_Components30,
trim(list_Of_Normalized_Media_Components31) AS list_Of_Normalized_Media_Components31,
trim(list_Of_Normalized_Media_Components32) AS list_Of_Normalized_Media_Components32,
trim(list_Of_Normalized_Media_Components33) AS list_Of_Normalized_Media_Components33,
trim(list_Of_Normalized_Media_Components34) AS list_Of_Normalized_Media_Components34,
trim(list_Of_Normalized_Media_Components35) AS list_Of_Normalized_Media_Components35,
trim(list_Of_Normalized_Media_Components36) AS list_Of_Normalized_Media_Components36,
trim(list_Of_Normalized_Media_Components37) AS list_Of_Normalized_Media_Components37,
trim(list_Of_Normalized_Media_Components38) AS list_Of_Normalized_Media_Components38,
trim(list_Of_Normalized_Media_Components39) AS list_Of_Normalized_Media_Components39,
trim(list_Of_Normalized_Media_Components40) AS list_Of_Normalized_Media_Components40,
trim(list_Of_Normalized_Media_Components41) AS list_Of_Normalized_Media_Components41,
trim(list_Of_Normalized_Media_Components42) AS list_Of_Normalized_Media_Components42,
trim(list_Of_Normalized_Media_Components43) AS list_Of_Normalized_Media_Components43,
trim(list_Of_Normalized_Media_Components44) AS list_Of_Normalized_Media_Components44,
trim(list_Of_Normalized_Media_Components45) AS list_Of_Normalized_Media_Components45,
trim(list_Of_Normalized_Media_Components46) AS list_Of_Normalized_Media_Components46,
trim(list_Of_Normalized_Media_Components47) AS list_Of_Normalized_Media_Components47,
trim(list_Of_Normalized_Media_Components48) AS list_Of_Normalized_Media_Components48,
trim(list_Of_Normalized_Media_Components49) AS list_Of_Normalized_Media_Components49,
trim(list_Of_Normalized_Media_Components50) AS list_Of_Normalized_Media_Components50,
CONCAT(if(LENGTH(trim(list_Of_Normalized_Media_Components6)) > 0,'LMC6',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components7)) > 0,'LMC7',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components8)) > 0,'LMC8',''), 
if(LENGTH(trim(list_Of_Normalized_Media_Components9)) > 0,'LMC9',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components10)) > 0,'LMC10',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components11)) > 0,'LMC11',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components12)) > 0,'LMC12',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components13)) > 0,'LMC13',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components14)) > 0,'LMC14',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components15)) > 0,'LMC15',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components16)) > 0,'LMC16',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components17)) > 0,'LMC17',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components18)) > 0,'LMC18',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components19)) > 0,'LMC19',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components20)) > 0,'LMC20',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components21)) > 0,'LMC21',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components22)) > 0,'LMC22',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components23)) > 0,'LMC23',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components24)) > 0,'LMC24',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components25)) > 0,'LMC25',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components26)) > 0,'LMC26',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components27)) > 0,'LMC27',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components28)) > 0,'LMC28',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components29)) > 0,'LMC29',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components30)) > 0,'LMC30',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components31)) > 0,'LMC31',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components32)) > 0,'LMC32',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components33)) > 0,'LMC33',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components34)) > 0,'LMC34',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components35)) > 0,'LMC35',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components36)) > 0,'LMC36',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components37)) > 0,'LMC37',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components38)) > 0,'LMC38',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components39)) > 0,'LMC39',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components40)) > 0,'LMC40',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components41)) > 0,'LMC41',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components42)) > 0,'LMC42',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components43)) > 0,'LMC43',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components44)) > 0,'LMC44',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components45)) > 0,'LMC45',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components46)) > 0,'LMC46',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components47)) > 0,'LMC47',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components48)) > 0,'LMC48',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components49)) > 0,'LMC49',''),
if(LENGTH(trim(list_Of_Normalized_Media_Components50)) > 0,'LMC50','')) as ListOfNrmlMedCompts1150,
trim(gGSNaddress) AS GGSNAddress,
trim(serviceReasonReturnCode) AS ServiceReasonReturnCode,
trim(list_Of_Message_Bodies) AS LstOfMessageBodies,
trim(RecordExtension) AS RecordExtension,
trim(Expires) AS Expires,
trim(Event) AS Event,
if(listOfAccessNetworkInformation1 is not NULL and length(listOfAccessNetworkInformation1) > 0,if(split(listOfAccessNetworkInformation1,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation1,'\\|')[0]) > 0,split(listOfAccessNetworkInformation1,'\\|')[0],NULL),NULL) as Lst1AccessNetworkInfo,
if(listOfAccessNetworkInformation1 is not NULL and length(listOfAccessNetworkInformation1) > 0,if(split(listOfAccessNetworkInformation1,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation1,'\\|')[1]) > 0,split(listOfAccessNetworkInformation1,'\\|')[1],NULL),NULL) as Lst1AccessDomain,
if(listOfAccessNetworkInformation1 is not NULL and length(listOfAccessNetworkInformation1) > 0,if(split(listOfAccessNetworkInformation1,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation1,'\\|')[2]) > 0,split(listOfAccessNetworkInformation1,'\\|')[2],NULL),NULL) as Lst1AccessType,
if(listOfAccessNetworkInformation1 is not NULL and length(listOfAccessNetworkInformation1) > 0,if(split(listOfAccessNetworkInformation1,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation1,'\\|')[3]) > 0,split(listOfAccessNetworkInformation1,'\\|')[3],NULL),NULL) as Lst1LocationInfoType,
if(listOfAccessNetworkInformation1 is not NULL and length(listOfAccessNetworkInformation1) > 0,if(split(listOfAccessNetworkInformation1,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation1,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation1,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation1,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation1,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst1ChangeTime,  																																																																											
if(listOfAccessNetworkInformation1 is not NULL and length(listOfAccessNetworkInformation1) > 0,if(split(listOfAccessNetworkInformation1,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation1,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation1,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation1,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation1,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst1ChangeTimeNormalized,
if(listOfAccessNetworkInformation2 is not NULL and length(listOfAccessNetworkInformation2) > 0,if(split(listOfAccessNetworkInformation2,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation2,'\\|')[0]) > 0,split(listOfAccessNetworkInformation2,'\\|')[0],NULL),NULL) as Lst2AccessNetworkInfo,
if(listOfAccessNetworkInformation2 is not NULL and length(listOfAccessNetworkInformation2) > 0,if(split(listOfAccessNetworkInformation2,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation2,'\\|')[1]) > 0,split(listOfAccessNetworkInformation2,'\\|')[1],NULL),NULL) as Lst2AccessDomain,
if(listOfAccessNetworkInformation2 is not NULL and length(listOfAccessNetworkInformation2) > 0,if(split(listOfAccessNetworkInformation2,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation2,'\\|')[2]) > 0,split(listOfAccessNetworkInformation2,'\\|')[2],NULL),NULL) as Lst2AccessType,
if(listOfAccessNetworkInformation2 is not NULL and length(listOfAccessNetworkInformation2) > 0,if(split(listOfAccessNetworkInformation2,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation2,'\\|')[3]) > 0,split(listOfAccessNetworkInformation2,'\\|')[3],NULL),NULL) as Lst2LocationInfoType,
if(listOfAccessNetworkInformation2 is not NULL and length(listOfAccessNetworkInformation2) > 0,if(split(listOfAccessNetworkInformation2,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation2,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation2,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation2,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation2,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst2ChangeTime,
if(listOfAccessNetworkInformation2 is not NULL and length(listOfAccessNetworkInformation2) > 0,if(split(listOfAccessNetworkInformation2,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation2,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation2,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation2,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation2,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst2ChangeTimeNormalized,
if(listOfAccessNetworkInformation3 is not NULL and length(listOfAccessNetworkInformation3) > 0,if(split(listOfAccessNetworkInformation3,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation3,'\\|')[0]) > 0,split(listOfAccessNetworkInformation3,'\\|')[0],NULL),NULL) as Lst3AccessNetworkInfo,
if(listOfAccessNetworkInformation3 is not NULL and length(listOfAccessNetworkInformation3) > 0,if(split(listOfAccessNetworkInformation3,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation3,'\\|')[1]) > 0,split(listOfAccessNetworkInformation3,'\\|')[1],NULL),NULL) as Lst3AccessDomain,
if(listOfAccessNetworkInformation3 is not NULL and length(listOfAccessNetworkInformation3) > 0,if(split(listOfAccessNetworkInformation3,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation3,'\\|')[2]) > 0,split(listOfAccessNetworkInformation3,'\\|')[2],NULL),NULL) as Lst3AccessType,
if(listOfAccessNetworkInformation3 is not NULL and length(listOfAccessNetworkInformation3) > 0,if(split(listOfAccessNetworkInformation3,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation3,'\\|')[3]) > 0,split(listOfAccessNetworkInformation3,'\\|')[3],NULL),NULL) as Lst3LocationInfoType,
if(listOfAccessNetworkInformation3 is not NULL and length(listOfAccessNetworkInformation3) > 0,if(split(listOfAccessNetworkInformation3,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation3,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation3,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation3,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation3,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst3ChangeTime,
if(listOfAccessNetworkInformation3 is not NULL and length(listOfAccessNetworkInformation3) > 0,if(split(listOfAccessNetworkInformation3,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation3,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation3,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation3,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation3,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst3ChangeTimeNormalized,
if(listOfAccessNetworkInformation4 is not NULL and length(listOfAccessNetworkInformation4) > 0,if(split(listOfAccessNetworkInformation4,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation4,'\\|')[0]) > 0,split(listOfAccessNetworkInformation4,'\\|')[0],NULL),NULL) as Lst4AccessNetworkInfo,
if(listOfAccessNetworkInformation4 is not NULL and length(listOfAccessNetworkInformation4) > 0,if(split(listOfAccessNetworkInformation4,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation4,'\\|')[1]) > 0,split(listOfAccessNetworkInformation4,'\\|')[1],NULL),NULL) as Lst4AccessDomain,
if(listOfAccessNetworkInformation4 is not NULL and length(listOfAccessNetworkInformation4) > 0,if(split(listOfAccessNetworkInformation4,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation4,'\\|')[2]) > 0,split(listOfAccessNetworkInformation4,'\\|')[2],NULL),NULL) as Lst4AccessType,
if(listOfAccessNetworkInformation4 is not NULL and length(listOfAccessNetworkInformation4) > 0,if(split(listOfAccessNetworkInformation4,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation4,'\\|')[3]) > 0,split(listOfAccessNetworkInformation4,'\\|')[3],NULL),NULL) as Lst4LocationInfoType,
if(listOfAccessNetworkInformation4 is not NULL and length(listOfAccessNetworkInformation4) > 0,if(split(listOfAccessNetworkInformation4,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation4,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation4,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation4,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation4,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst4ChangeTime,
if(listOfAccessNetworkInformation4 is not NULL and length(listOfAccessNetworkInformation4) > 0,if(split(listOfAccessNetworkInformation4,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation4,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation4,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation4,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation4,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst4ChangeTimeNormalized,
if(listOfAccessNetworkInformation5 is not NULL and length(listOfAccessNetworkInformation5) > 0,if(split(listOfAccessNetworkInformation5,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation5,'\\|')[0]) > 0,split(listOfAccessNetworkInformation5,'\\|')[0],NULL),NULL) as Lst5AccessNetworkInfo,
if(listOfAccessNetworkInformation5 is not NULL and length(listOfAccessNetworkInformation5) > 0,if(split(listOfAccessNetworkInformation5,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation5,'\\|')[1]) > 0,split(listOfAccessNetworkInformation5,'\\|')[1],NULL),NULL) as Lst5AccessDomain,
if(listOfAccessNetworkInformation5 is not NULL and length(listOfAccessNetworkInformation5) > 0,if(split(listOfAccessNetworkInformation5,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation5,'\\|')[2]) > 0,split(listOfAccessNetworkInformation5,'\\|')[2],NULL),NULL) as Lst5AccessType,
if(listOfAccessNetworkInformation5 is not NULL and length(listOfAccessNetworkInformation5) > 0,if(split(listOfAccessNetworkInformation5,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation5,'\\|')[3]) > 0,split(listOfAccessNetworkInformation5,'\\|')[3],NULL),NULL) as Lst5LocationInfoType,
if(listOfAccessNetworkInformation5 is not NULL and length(listOfAccessNetworkInformation5) > 0,if(split(listOfAccessNetworkInformation5,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation5,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation5,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation5,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation5,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst5ChangeTime,
if(listOfAccessNetworkInformation5 is not NULL and length(listOfAccessNetworkInformation5) > 0,if(split(listOfAccessNetworkInformation5,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation5,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation5,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation5,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation5,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst5ChangeTimeNormalized,
if(listOfAccessNetworkInformation6 is not NULL and length(listOfAccessNetworkInformation6) > 0,if(split(listOfAccessNetworkInformation6,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation6,'\\|')[0]) > 0,split(listOfAccessNetworkInformation6,'\\|')[0],NULL),NULL) as Lst6AccessNetworkInfo,
if(listOfAccessNetworkInformation6 is not NULL and length(listOfAccessNetworkInformation6) > 0,if(split(listOfAccessNetworkInformation6,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation6,'\\|')[1]) > 0,split(listOfAccessNetworkInformation6,'\\|')[1],NULL),NULL) as Lst6AccessDomain,
if(listOfAccessNetworkInformation6 is not NULL and length(listOfAccessNetworkInformation6) > 0,if(split(listOfAccessNetworkInformation6,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation6,'\\|')[2]) > 0,split(listOfAccessNetworkInformation6,'\\|')[2],NULL),NULL) as Lst6AccessType,
if(listOfAccessNetworkInformation6 is not NULL and length(listOfAccessNetworkInformation6) > 0,if(split(listOfAccessNetworkInformation6,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation6,'\\|')[3]) > 0,split(listOfAccessNetworkInformation6,'\\|')[3],NULL),NULL) as Lst6LocationInfoType,
if(listOfAccessNetworkInformation6 is not NULL and length(listOfAccessNetworkInformation6) > 0,if(split(listOfAccessNetworkInformation6,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation6,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation6,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation6,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation6,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst6ChangeTime,
if(listOfAccessNetworkInformation6 is not NULL and length(listOfAccessNetworkInformation6) > 0,if(split(listOfAccessNetworkInformation6,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation6,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation6,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation6,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation6,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst6ChangeTimeNormalized,
if(listOfAccessNetworkInformation7 is not NULL and length(listOfAccessNetworkInformation7) > 0,if(split(listOfAccessNetworkInformation7,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation7,'\\|')[0]) > 0,split(listOfAccessNetworkInformation7,'\\|')[0],NULL),NULL) as Lst7AccessNetworkInfo,
if(listOfAccessNetworkInformation7 is not NULL and length(listOfAccessNetworkInformation7) > 0,if(split(listOfAccessNetworkInformation7,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation7,'\\|')[1]) > 0,split(listOfAccessNetworkInformation7,'\\|')[1],NULL),NULL) as Lst7AccessDomain,
if(listOfAccessNetworkInformation7 is not NULL and length(listOfAccessNetworkInformation7) > 0,if(split(listOfAccessNetworkInformation7,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation7,'\\|')[2]) > 0,split(listOfAccessNetworkInformation7,'\\|')[2],NULL),NULL) as Lst7AccessType,
if(listOfAccessNetworkInformation7 is not NULL and length(listOfAccessNetworkInformation7) > 0,if(split(listOfAccessNetworkInformation7,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation7,'\\|')[3]) > 0,split(listOfAccessNetworkInformation7,'\\|')[3],NULL),NULL) as Lst7LocationInfoType,
if(listOfAccessNetworkInformation7 is not NULL and length(listOfAccessNetworkInformation7) > 0,if(split(listOfAccessNetworkInformation7,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation7,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation7,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation7,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation7,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst7ChangeTime,
if(listOfAccessNetworkInformation7 is not NULL and length(listOfAccessNetworkInformation7) > 0,if(split(listOfAccessNetworkInformation7,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation7,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation7,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation7,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation7,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst7ChangeTimeNormalized,
if(listOfAccessNetworkInformation8 is not NULL and length(listOfAccessNetworkInformation8) > 0,if(split(listOfAccessNetworkInformation8,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation8,'\\|')[0]) > 0,split(listOfAccessNetworkInformation8,'\\|')[0],NULL),NULL) as Lst8AccessNetworkInfo,
if(listOfAccessNetworkInformation8 is not NULL and length(listOfAccessNetworkInformation8) > 0,if(split(listOfAccessNetworkInformation8,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation8,'\\|')[1]) > 0,split(listOfAccessNetworkInformation8,'\\|')[1],NULL),NULL) as Lst8AccessDomain,
if(listOfAccessNetworkInformation8 is not NULL and length(listOfAccessNetworkInformation8) > 0,if(split(listOfAccessNetworkInformation8,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation8,'\\|')[2]) > 0,split(listOfAccessNetworkInformation8,'\\|')[2],NULL),NULL) as Lst8AccessType,
if(listOfAccessNetworkInformation8 is not NULL and length(listOfAccessNetworkInformation8) > 0,if(split(listOfAccessNetworkInformation8,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation8,'\\|')[3]) > 0,split(listOfAccessNetworkInformation8,'\\|')[3],NULL),NULL) as Lst8LocationInfoType,
if(listOfAccessNetworkInformation8 is not NULL and length(listOfAccessNetworkInformation8) > 0,if(split(listOfAccessNetworkInformation8,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation8,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation8,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation8,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation8,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst8ChangeTime,
if(listOfAccessNetworkInformation8 is not NULL and length(listOfAccessNetworkInformation8) > 0,if(split(listOfAccessNetworkInformation8,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation8,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation8,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation8,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation8,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst8ChangeTimeNormalized,
if(listOfAccessNetworkInformation9 is not NULL and length(listOfAccessNetworkInformation9) > 0,if(split(listOfAccessNetworkInformation9,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation9,'\\|')[0]) > 0,split(listOfAccessNetworkInformation9,'\\|')[0],NULL),NULL) as Lst9AccessNetworkInfo,
if(listOfAccessNetworkInformation9 is not NULL and length(listOfAccessNetworkInformation9) > 0,if(split(listOfAccessNetworkInformation9,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation9,'\\|')[1]) > 0,split(listOfAccessNetworkInformation9,'\\|')[1],NULL),NULL) as Lst9AccessDomain,
if(listOfAccessNetworkInformation9 is not NULL and length(listOfAccessNetworkInformation9) > 0,if(split(listOfAccessNetworkInformation9,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation9,'\\|')[2]) > 0,split(listOfAccessNetworkInformation9,'\\|')[2],NULL),NULL) as Lst9AccessType,
if(listOfAccessNetworkInformation9 is not NULL and length(listOfAccessNetworkInformation9) > 0,if(split(listOfAccessNetworkInformation9,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation9,'\\|')[3]) > 0,split(listOfAccessNetworkInformation9,'\\|')[3],NULL),NULL) as Lst9LocationInfoType,
if(listOfAccessNetworkInformation9 is not NULL and length(listOfAccessNetworkInformation9) > 0,if(split(listOfAccessNetworkInformation9,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation9,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation9,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation9,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation9,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst9ChangeTime,
if(listOfAccessNetworkInformation9 is not NULL and length(listOfAccessNetworkInformation9) > 0,if(split(listOfAccessNetworkInformation9,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation9,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation9,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation9,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation9,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst9ChangeTimeNormalized,
if(listOfAccessNetworkInformation10 is not NULL and length(listOfAccessNetworkInformation10) > 0,if(split(listOfAccessNetworkInformation10,'\\|')[0] is not null and length(split(listOfAccessNetworkInformation10,'\\|')[0]) > 0,split(listOfAccessNetworkInformation10,'\\|')[0],NULL),NULL) as Lst10AccessNetworkInfo,
if(listOfAccessNetworkInformation10 is not NULL and length(listOfAccessNetworkInformation10) > 0,if(split(listOfAccessNetworkInformation10,'\\|')[1] is not null and length(split(listOfAccessNetworkInformation10,'\\|')[1]) > 0,split(listOfAccessNetworkInformation10,'\\|')[1],NULL),NULL) as Lst10AccessDomain,
if(listOfAccessNetworkInformation10 is not NULL and length(listOfAccessNetworkInformation10) > 0,if(split(listOfAccessNetworkInformation10,'\\|')[2] is not null and length(split(listOfAccessNetworkInformation10,'\\|')[2]) > 0,split(listOfAccessNetworkInformation10,'\\|')[2],NULL),NULL) as Lst10AccessType,
if(listOfAccessNetworkInformation10 is not NULL and length(listOfAccessNetworkInformation10) > 0,if(split(listOfAccessNetworkInformation10,'\\|')[3] is not null and length(split(listOfAccessNetworkInformation10,'\\|')[3]) > 0,split(listOfAccessNetworkInformation10,'\\|')[3],NULL),NULL) as Lst10LocationInfoType,
if(listOfAccessNetworkInformation10 is not NULL and length(listOfAccessNetworkInformation10) > 0,if(split(listOfAccessNetworkInformation10,'\\|')[4] is not null and length(split(listOfAccessNetworkInformation10,'\\|')[4]) > 0,if((split(listOfAccessNetworkInformation10,'\\|')[4] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation10,'\\|')[4]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation10,'\\|')[4], 1, 14), 'yyyyMMddHHmmss')),NULL),NULL),NULL) as Lst10ChangeTime,
if(listOfAccessNetworkInformation10 is not NULL and length(listOfAccessNetworkInformation10) > 0,if(split(listOfAccessNetworkInformation10,'\\|')[5] is not null and length(split(listOfAccessNetworkInformation10,'\\|')[5]) > 0,if((split(listOfAccessNetworkInformation10,'\\|')[5] is not NULL) AND LENGTH(split(listOfAccessNetworkInformation10,'\\|')[5]) > 13,from_unixtime(unix_timestamp(substr(split(listOfAccessNetworkInformation10,'\\|')[5], 1, 14), 'yyyyMMddHHmmss')), NULL),NULL),NULL) as Lst10ChangeTimeNormalized,
trim(ServiceContextID) AS ServiceContextID,
--Phase 2 changes starts
trim(list_of_subscription_ID1) AS SubscriberE164_orig,
--Phase 2 changes ends
if((list_of_subscription_ID1 is not NULL) AND length(list_of_subscription_ID1) > 0,if(INSTR(UPPER(split(list_of_subscription_ID1,'\\|')[1]),'SIP:+') > 0 OR INSTR(UPPER(split(list_of_subscription_ID1,'\\|')[1]),'TEL:+') > 0, if(instr(trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1),'@') - 1)) , '.') = 0 or instr( trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1),'@') - 1)), '-') = 0 or instr( trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1),'@') - 1)), '+') = 0, cast(trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1),'@') - 1)) as bigint), NULL),if(INSTR(UPPER(split(list_of_subscription_ID1,'\\|')[1]),'SIP:') > 0 or INSTR(UPPER(split(list_of_subscription_ID1,'\\|')[1]),'TEL:') > 0, if(instr(trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1),'@') - 1)) , '.') = 0 or instr( trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1),'@') - 1)), '-') = 0 or instr( trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1),'@') - 1)), '+') = 0, cast(trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1, INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1], INSTR(split(list_of_subscription_ID1,'\\|')[1],':') + 1),'@') - 1)) as bigint), NULL),if(substring(split(list_of_subscription_ID1,'\\|')[1], 1, 1)='+',substr(split(list_of_subscription_ID1,'\\|')[1],2),cast(trim(split(list_of_subscription_ID1,'\\|')[1]) as bigint)))),NULL) AS SubscriberE164,
if((list_of_subscription_ID1 is not NULL) AND length(list_of_subscription_ID1) > 0,if(INSTR(split(list_of_subscription_ID1,'\\|')[1],'sip:') > 0,REGEXP_EXTRACT(trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1,INSTR(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1),'@') - 1)), '(1*)(.*)', 2),if(INSTR(split(list_of_subscription_ID1,'\\|')[1],'tel:') > 0,REGEXP_EXTRACT(trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1)), '(1*)(.*)', 2),if(INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') > 0,REGEXP_EXTRACT(trim(SUBSTR(split(list_of_subscription_ID1,'\\|')[1],INSTR(split(list_of_subscription_ID1,'\\|')[1],'+') + 1)),'(1*)(.*)', 2),REGEXP_EXTRACT(split(list_of_subscription_ID1,'\\|')[1],'(1*)(.*)', 2)))),-1) AS SubscriberNo,
if((list_of_subscription_ID2 is not NULL) AND length(list_of_subscription_ID2) > 0, SUBSTR(list_of_subscription_ID2, INSTR(list_of_subscription_ID2,'|') + 1),NULL) as IMSI,
if((list_of_subscription_ID3 is not NULL) AND length(list_of_subscription_ID3) > 0, SUBSTR(list_of_subscription_ID3, INSTR(list_of_subscription_ID3,'|') + 1),NULL) as IMEI,
if((list_of_subscription_ID4 is not NULL) AND length(list_of_subscription_ID4) > 0, SUBSTR(list_of_subscription_ID4, INSTR(list_of_subscription_ID4,'|') + 1),NULL) as SubSIPURI,
if((list_of_subscription_ID5 is not NULL) AND length(list_of_subscription_ID5) > 0, SUBSTR(list_of_subscription_ID5, INSTR(list_of_subscription_ID5,'|') + 1),NULL) as NAI,
if((list_of_subscription_ID6 is not NULL) AND length(list_of_subscription_ID6) > 0, SUBSTR(list_of_subscription_ID6, INSTR(list_of_subscription_ID6,'|') + 1),NULL) as SubPrivate,
if((list_of_subscription_ID7 is not NULL) AND length(list_of_subscription_ID7) > 0, SUBSTR(list_of_subscription_ID7, INSTR(list_of_subscription_ID7,'|') + 1),NULL) as SubServedPartyDeviceID,
--Phase 2 changes starts
trim(list_of_subscription_ID8) AS OtherPartyAddress_orig,
--Phase 2 changes ends
if((list_of_subscription_ID8 is not NULL) AND length(list_of_subscription_ID8) > 0,if(INSTR(split(list_of_subscription_ID8,'\\|')[1],'sip:') > 0,trim(SUBSTR(split(list_of_subscription_ID8,'\\|')[1],INSTR(split(list_of_subscription_ID8,'\\|')[1],'+') + 1,INSTR(SUBSTR(split(list_of_subscription_ID8,'\\|')[1],INSTR(split(list_of_subscription_ID8,'\\|')[1],'+') + 1),'@') - 1)),if(INSTR(split(list_of_subscription_ID8,'\\|')[1],'tel:') > 0,trim(SUBSTR(split(list_of_subscription_ID8,'\\|')[1],INSTR(split(list_of_subscription_ID8,'\\|')[1],'+') + 1)),if(INSTR(split(list_of_subscription_ID8,'\\|')[1],'+') > 0,trim(SUBSTR(split(list_of_subscription_ID8,'\\|')[1],INSTR(split(list_of_subscription_ID8,'\\|')[1],'+') + 1)),split(list_of_subscription_ID8,'\\|')[1]))),NULL) as OtherPartyAddress,
if((list_of_subscription_ID9 is not NULL) AND length(list_of_subscription_ID9) > 0, SUBSTR(list_of_subscription_ID9, INSTR(list_of_subscription_ID9,'|') + 1),NULL) as PhoneType, --new field
if((list_of_subscription_ID10 is not NULL) AND length(list_of_subscription_ID10) > 0, SUBSTR(list_of_subscription_ID10, INSTR(list_of_subscription_ID10,'|') + 1),NULL) as ServiceProvider, --new field
if((list_of_subscription_ID11 is not NULL) AND length(list_of_subscription_ID11) > 0, SUBSTR(list_of_subscription_ID11, INSTR(list_of_subscription_ID11,'|') + 1),NULL) as HG_GROUP, --new field
--Phase 2 changes starts
trim(list_of_subscription_ID12) AS End_User_SubsID_orig,
----End_User_SubsID_CTN LOGIC BEGINS-----
if((list_of_subscription_ID12 is not NULL) AND length(list_of_subscription_ID12) > 0,
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:+gp') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+gp') + 7, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+gp') + 7),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:+puiv') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+puiv') + 9, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+puiv') + 9),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:+pui') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+pui') + 8, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+pui') + 8),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:+') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+') + 5, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:+') + 5),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:gp') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:gp') + 6, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:gp') + 6),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:puiv') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:puiv') + 8, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:puiv') + 8),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:pui') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:pui') + 7, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:pui') + 7),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'sip:') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:') + 4, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'sip:') + 4),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:+gp') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+gp') + 7, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+gp') + 7),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:+puiv') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+puiv') + 9, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+puiv') + 9),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:+pui') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+pui') + 8, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+pui') + 8),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:+') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+') + 5, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:+') + 5),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:gp') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:gp') + 6, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:gp') + 6),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:puiv') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:puiv') + 8, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:puiv') + 8),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:pui') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:pui') + 7, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:pui') + 7),'@') - 1)),
if(INSTR(LOWER(split(list_of_subscription_ID12,'\\|')[1]),'tel:') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:') + 4, INSTR(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]),'tel:') + 4),'@') - 1)),
if(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], 1, 1) = '+',SUBSTR(split(list_of_subscription_ID12,'\\|')[1], 2),
if(INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]), '@') > 0,SUBSTR(split(list_of_subscription_ID12,'\\|')[1], 1,INSTR(Lower(split(list_of_subscription_ID12,'\\|')[1]), '@')-1),
Split(list_of_subscription_ID12,'\\|')[1]))))))))))))))))))
,NULL) AS End_User_SubsID_CTN,
----End_User_SubsID_CTN LOGIC ENDS------
--End_User_SubsID_Dom_fl LOGIC
if((list_of_subscription_ID12 is not NULL) AND length(list_of_subscription_ID12) > 0, if(INSTR(split(list_of_subscription_ID12,'\\|')[1],'@') > 0,trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(split(list_of_subscription_ID12,'\\|')[1],'@')+1)), NULL), NULL) AS End_User_SubsID_Dom_fl,
--End_User_SubsID_Dom_ty LOGIC
if((list_of_subscription_ID12 is not NULL) AND length(list_of_subscription_ID12) > 0, if(INSTR(split(list_of_subscription_ID12,'\\|')[1],'@') > 0, trim(SUBSTR(split(list_of_subscription_ID12,'\\|')[1], INSTR(split(list_of_subscription_ID12,'\\|')[1],'@')+1, (INSTR(split(list_of_subscription_ID12,'\\|')[1],'.') -1) - INSTR(split(list_of_subscription_ID12,'\\|')[1],'@'))), NULL), NULL) AS End_User_SubsID_Dom_ty,
--Phase 2 changes ends
trim(list_Of_Early_SDP_Media_Components) AS LstOfEarlySDPMediaCmpnts,
trim(iMSCommunicationServiceIdentifier) AS IMSCommServiceIdentifier,
trim(NumberPortabilityRouting) AS NumberPortabilityRouting,
trim(CarrierSelectRouting) AS CarrierSelectRouting,
trim(SessionPriority) AS SessionPriority,
--Phase 2 changes starts
trim(requested_Party_Address) AS RequestedPartyAddress_orig,
--Phase 2 changes ends
--RequestedPartyAddress LOGIC starts--
if((requested_Party_Address is not NULL) AND length(requested_Party_Address) > 0,
if(INSTR(LOWER(requested_Party_Address),'sip:gp') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:gp') + 6),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:gp') + 6, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:gp') + 6),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:gp') + 6))),
if(INSTR(LOWER(requested_Party_Address),'sip:puiv') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:puiv') + 8),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:puiv') + 8, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:puiv') + 8),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:puiv') + 8))),
if(INSTR(LOWER(requested_Party_Address),'sip:pui') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:pui') + 7),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:pui') + 7, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:pui') + 7),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:pui') + 7))),
if(INSTR(LOWER(requested_Party_Address),'sip:') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:') + 4),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:') + 4, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:') + 4),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'sip:') + 4))),
if(INSTR(LOWER(requested_Party_Address),'tel:gp') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:gp') + 6),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:gp') + 6, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:gp') + 6),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:gp') + 6))),
if(INSTR(LOWER(requested_Party_Address),'tel:puiv') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:puiv') + 8),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:puiv') + 8, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:puiv') + 8),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:puiv') + 8))),
if(INSTR(LOWER(requested_Party_Address),'tel:pui') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:pui') + 7),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:pui') + 7, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:pui') + 7),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:pui') + 7))),
if(INSTR(LOWER(requested_Party_Address),'tel:') > 0,if(INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:') + 4),'@') > 0,trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:') + 4, INSTR(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:') + 4),'@') - 1)),trim(SUBSTR(requested_Party_Address, INSTR(Lower(requested_Party_Address),'tel:') + 4))),
if(SUBSTR(requested_Party_Address, 1, 1) = '+',SUBSTR(requested_Party_Address, 1),
requested_Party_Address)))))))))
,NULL) AS RequestedPartyAddress,
---RequestedPartyAddress LOGIC ends--
trim(list_Of_Called_Asserted_Identity) AS LstOfCalledAssertedID,
trim(mMTelInformation_analyzed_Call_Type) AS MMTelInfoAnalyzedCallType,
trim(mMTelInformation_called_Asserted_Identity_Presentation_Status) AS MTlInfoCalledAsrtIDPrsntSts,
trim(mMTelInformation_calling_Party_Address_Presentation_Status) AS MTlInfoCllgPrtyAddrPrsntSts,
trim(mMTelInformation_conference_Id) AS MMTelInfoConferenceId,
trim(mMTelInformation_dial_Around_Indicator) AS MMTelInfoDialAroundIndctr,
trim(mMTelInformation_related_ICID) AS MMTelInfoRelatedICID,
--Phase 2 changes starts - Modified logic
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information1,'\\|')[0] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information1 is not NULL and length(mMTelInformation_supplementary_Service_Information1) > 0, if(length(split(mMTelInformation_supplementary_Service_Information1,'\\|')[0]) > 0,split(mMTelInformation_supplementary_Service_Information1,'\\|')[0],NULL),NULL) as MMTelSplmtrySrvcInfo1ID,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information1,'\\|')[1] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information1 is not NULL and length(mMTelInformation_supplementary_Service_Information1) > 0, if(split(mMTelInformation_supplementary_Service_Information1,'\\|')[1] is not NULL and length(split(mMTelInformation_supplementary_Service_Information1,'\\|')[1]) > 0,split(mMTelInformation_supplementary_Service_Information1,'\\|')[1],NULL),NULL) as MMTelSplmtrySrvcInfo1Act,
--Phase 2 changes ends - Modified logic
--Phase 2 changes starts
trim(mMTelInformation_supplementary_Service_Information1)AS MMTelSplmtrySrvcInfo1Redir_orig,
--Phase 2 changes ends
--Phase 2 changes starts - Modified logic
--modified to populate NULL when split(mMTelInformation_supplementary_Service_Information1,'\\|')[2] has blank or invalid values after the split
if((mMTelInformation_supplementary_Service_Information1 is not NULL) AND length(mMTelInformation_supplementary_Service_Information1) > 0,if(INSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],'sip:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],':') + 1,INSTR(SUBSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],':') + 1),'@') - 1)),if(INSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],'tel:v') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],':v') + 2)),if(INSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],'tel:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],':') + 1)),if(INSTR(Substr(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2], 1,1),'+') > 0,trim(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2]),if(length(split(mMTelInformation_supplementary_Service_Information1,'\\|')[2]) > 0,split(mMTelInformation_supplementary_Service_Information1,'\\|')[2],NULL))))),NULL) as MMTelSplmtrySrvcInfo1Redir,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information2,'\\|')[0] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information2 is not NULL and length(mMTelInformation_supplementary_Service_Information2) > 0,if(length(split(mMTelInformation_supplementary_Service_Information2,'\\|')[0]) > 0,split(mMTelInformation_supplementary_Service_Information2,'\\|')[0],NULL),NULL) as MMTelSplmtrySrvcInfo2ID,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information2,'\\|')[1] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information2 is not NULL and length(mMTelInformation_supplementary_Service_Information2) > 0,if(split(mMTelInformation_supplementary_Service_Information2,'\\|')[1] is not NULL and length(split(mMTelInformation_supplementary_Service_Information2,'\\|')[1]) > 0,split(mMTelInformation_supplementary_Service_Information2,'\\|')[1],NULL),NULL) as MMTelSplmtrySrvcInfo2Act,
--Phase 2 changes ends - Modified logic
--Phase 2 changes starts
trim(mMTelInformation_supplementary_Service_Information2) AS MMTelSplmtrySrvcInfo2Redir_orig,
--Phase 2 changes ends
--Phase 2 changes starts - Modified logic
--modified to populate NULL when split(mMTelInformation_supplementary_Service_Information2,'\\|')[2] has blank or invalid values after the split
if((mMTelInformation_supplementary_Service_Information2 is not NULL) AND length(mMTelInformation_supplementary_Service_Information2) > 0,if(INSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],'sip:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],':') + 1,INSTR(SUBSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],':') + 1),'@') - 1)),if(INSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],'tel:v') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],':v') + 2)),if(INSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],'tel:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],':') + 1)),if(INSTR(Substr(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2], 1,1),'+') > 0,trim(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2]),if(length(split(mMTelInformation_supplementary_Service_Information2,'\\|')[2]) > 0,split(mMTelInformation_supplementary_Service_Information2,'\\|')[2],NULL))))),NULL) as MMTelSplmtrySrvcInfo2Redir,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information3,'\\|')[0] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information3 is not NULL and length(mMTelInformation_supplementary_Service_Information3) > 0,if(length(split(mMTelInformation_supplementary_Service_Information3,'\\|')[0]) > 0,split(mMTelInformation_supplementary_Service_Information3,'\\|')[0],NULL),NULL) as MMTelSplmtrySrvcInfo3ID,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information3,'\\|')[1] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information3 is not NULL and length(mMTelInformation_supplementary_Service_Information3) > 0,if(split(mMTelInformation_supplementary_Service_Information3,'\\|')[1] is not NULL and length(split(mMTelInformation_supplementary_Service_Information3,'\\|')[1]) > 0,split(mMTelInformation_supplementary_Service_Information3,'\\|')[1],NULL),NULL) as MMTelSplmtrySrvcInfo3Act,
--Phase 2 changes ends - Modified logic
--Phase 2 changes starts
trim(mMTelInformation_supplementary_Service_Information3) AS MMTelSplmtrySrvcInfo3Redir_orig,
--Phase 2 changes ends
--Phase 2 changes starts - Modified logic
--modified to populate NULL when split(mMTelInformation_supplementary_Service_Information3,'\\|')[2] has blank or invalid values after the split
if((mMTelInformation_supplementary_Service_Information3 is not NULL) AND length(mMTelInformation_supplementary_Service_Information3) > 0,if(INSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],'sip:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],':') + 1,INSTR(SUBSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],':') + 1),'@') - 1)),if(INSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],'tel:v') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],':v') + 2)),if(INSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],'tel:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],':') + 1)),if(INSTR(Substr(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2], 1,1),'+') > 0,trim(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2]),if(length(split(mMTelInformation_supplementary_Service_Information3,'\\|')[2]) > 0,split(mMTelInformation_supplementary_Service_Information3,'\\|')[2],NULL))))),NULL) as MMTelSplmtrySrvcInfo3Redir,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information4,'\\|')[0] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information4 is not NULL and length(mMTelInformation_supplementary_Service_Information4) > 0,if(length(split(mMTelInformation_supplementary_Service_Information4,'\\|')[0]) > 0,split(mMTelInformation_supplementary_Service_Information4,'\\|')[0],NULL),NULL) as MMTelSplmtrySrvcInfo4ID,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information4,'\\|')[1] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information4 is not NULL and length(mMTelInformation_supplementary_Service_Information4) > 0,if(split(mMTelInformation_supplementary_Service_Information4,'\\|')[1] is not NULL and length(split(mMTelInformation_supplementary_Service_Information4,'\\|')[1]) > 0,split(mMTelInformation_supplementary_Service_Information4,'\\|')[1],NULL),NULL) as MMTelSplmtrySrvcInfo4Act,
--Phase 2 changes ends - Modified logic
--Phase 2 changes starts
trim(mMTelInformation_supplementary_Service_Information4) AS MMTelSplmtrySrvcInfo4Redir_orig,
--Phase 2 changes ends
--Phase 2 changes starts - Modified logic
--modified to populate NULL when split(mMTelInformation_supplementary_Service_Information4,'\\|')[2] has blank or invalid values after the split
if((mMTelInformation_supplementary_Service_Information4 is not NULL) AND length(mMTelInformation_supplementary_Service_Information4) > 0,if(INSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],'sip:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],':') + 1,INSTR(SUBSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],':') + 1),'@') - 1)),if(INSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],'tel:v') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],':v') + 2)),if(INSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],'tel:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],':') + 1)),if(INSTR(Substr(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2], 1,1),'+') > 0,trim(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2]),if(length(split(mMTelInformation_supplementary_Service_Information4,'\\|')[2]) > 0,split(mMTelInformation_supplementary_Service_Information4,'\\|')[2],NULL))))),NULL) as MMTelSplmtrySrvcInfo4Redir,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information5,'\\|')[0] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information5 is not NULL and length(mMTelInformation_supplementary_Service_Information5) > 0,if(length(split(mMTelInformation_supplementary_Service_Information5,'\\|')[0]) > 0,split(mMTelInformation_supplementary_Service_Information5,'\\|')[0],NULL),NULL) as MMTelSplmtrySrvcInfo5ID,
--Modified logic to populate NULL when split(mMTelInformation_supplementary_Service_Information5,'\\|')[1] has blank or invalid values after the split
if(mMTelInformation_supplementary_Service_Information5 is not NULL and length(mMTelInformation_supplementary_Service_Information5) > 0,if(split(mMTelInformation_supplementary_Service_Information5,'\\|')[1] is not NULL and length(split(mMTelInformation_supplementary_Service_Information5,'\\|')[1]) > 0,split(mMTelInformation_supplementary_Service_Information5,'\\|')[1],NULL),NULL) as MMTelSplmtrySrvcInfo5Act,
--Phase 2 changes ends - Modified logic
--Phase 2 changes starts
trim(mMTelInformation_supplementary_Service_Information5) AS MMTelSplmtrySrvcInfo5Redir_orig,
--Phase 2 changes ends
--Phase 2 changes starts - Modified logic
--modified to populate NULL when split(mMTelInformation_supplementary_Service_Information5,'\\|')[2] has blank or invalid values after the split
if((mMTelInformation_supplementary_Service_Information5 is not NULL) AND length(mMTelInformation_supplementary_Service_Information5) > 0,if(INSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],'sip:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],':') + 1,INSTR(SUBSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],':') + 1),'@') - 1)),if(INSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],'tel:v') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],':v') + 2)),if(INSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],'tel:') > 0,trim(SUBSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],INSTR(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],':') + 1)),if(INSTR(Substr(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2], 1,1),'+') > 0,trim(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2]),if(length(split(mMTelInformation_supplementary_Service_Information5,'\\|')[2]) > 0,split(mMTelInformation_supplementary_Service_Information5,'\\|')[2],NULL))))),NULL) as MMTelSplmtrySrvcInfo5Redir,
--Phase 2 changes ends - Modified logic
trim(mMTelInformation_supplementary_Service_Information6) AS mMTelInformation_supplementary_Service_Information6,
trim(mMTelInformation_supplementary_Service_Information7) AS mMTelInformation_supplementary_Service_Information7,
trim(mMTelInformation_supplementary_Service_Information8) AS mMTelInformation_supplementary_Service_Information8,
trim(mMTelInformation_supplementary_Service_Information9) AS mMTelInformation_supplementary_Service_Information9,
trim(mMTelInformation_supplementary_Service_Information10) AS mMTelInformation_supplementary_Service_Information10,
trim(MobileStationRoamingNumber) AS MobileStationRoamingNumber,
trim(TeleServiceCode) AS TeleServiceCode,
trim(TariffClass) AS TariffClass,
trim(PVisitedNetworkID) AS PVisitedNetworkID,
--Phase 2 changes starts
trim(networkCallType) AS networkCallType,
trim(carrierIdCode) AS carrierIdCode,
--Phase 2 changes ends
-- Control fields
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records
FROM cdrdm.ucc1205ascii a ${hiveconf:WHERE_clause}
;

-- PHASE 2 CHANGES - SOLUTION 2 SARTS FOR ucc1205ascii_temp2

-- Preparing TR_BAN, TR_ACCOUNT_SUB_TYPE, TR_COMPANY_CODE, TR_FRANCHISE_CD, TR_PRODUCT_TYPE, TR_BA_ACCOUNT_TYPE columns...
CREATE TABLE ext_cdrdm.ucc1205ascii_temp2 AS
SELECT 
 a.*,
-- NEW columns...
b.BAN AS TR_BAN,
b.ACCOUNT_SUB_TYPE AS TR_ACCOUNT_SUB_TYPE,
b.COMPANY_CODE AS TR_COMPANY_CODE,
b.FRANCHISE_CD AS TR_FRANCHISE_CD,
--Phase 2 changes starts
b.PRODUCT_TYPE AS TR_PRODUCT_TYPE,
--Phase 2 changes ends
b.TR_BA_ACCOUNT_TYPE AS TR_BA_ACCOUNT_TYPE,
-- Additional columns for processing. Not to include in Final table...
SUBSTR(trim(SubscriberE164),INSTR(trim(SubscriberE164),'|')+2) AS SUB_NO_E164,
SUBSTR(trim(OtherPartyAddress),INSTR(trim(OtherPartyAddress),'|')+2) AS SUB_NO_PartyAddr,
SUBSTR(trim(MMTelSplmtrySrvcInfo1Redir),INSTR(trim(MMTelSplmtrySrvcInfo1Redir),'|')+2) AS SUB_NO_Tel,
b.BAN AS BAN_TR_Sub_ALS_ind,
c.BAN AS BAN_TR_Sub_oth_party_ALS_ind,
d.BAN AS BAN_TR_Re_dir1_ALS_ind,

--Phase 2 changes starts
e.BAN AS WL_BAN,
e.ACCOUNT_SUB_TYPE AS WL_ACCOUNT_SUB_TYPE,
e.COMPANY_CODE AS WL_COMPANY_CODE,
e.FRANCHISE_CD AS WL_FRANCHISE_CD,
e.PRODUCT_TYPE AS WL_PRODUCT_TYPE,
e.TR_BA_ACCOUNT_TYPE AS WL_BA_ACCOUNT_TYPE,
e.BAN AS BAN_WL_Sub_ALS_ind,
f.BAN AS BAN_WL_Sub_oth_party_ALS_ind,
g.BAN AS BAN_WL_Re_dir1_ALS_ind
--Phase 2 changes ends
FROM ext_cdrdm.ucc1205ascii_temp1 a
LEFT OUTER JOIN (SELECT SUBSCRIBER_NO, BAN, ACCOUNT_SUB_TYPE, PRODUCT_TYPE, COMPANY_CODE, FRANCHISE_CD, TR_BA_ACCOUNT_TYPE FROM cdrdm.dim_mps_cust_1 WHERE MPS_CUST_SEQ_NO = 1 AND PRODUCT_TYPE = 'C') b ON (SUBSTR(TRIM(SubscriberE164),INSTR(TRIM(SubscriberE164),'|')+2) = TRIM(b.subscriber_no))
LEFT OUTER JOIN (SELECT SUBSCRIBER_NO, BAN FROM cdrdm.dim_mps_cust_1 WHERE MPS_CUST_SEQ_NO = 1 AND PRODUCT_TYPE = 'C') c ON (SUBSTR(trim(OtherPartyAddress),INSTR(trim(OtherPartyAddress),'|')+2) = trim(c.subscriber_no))
LEFT OUTER JOIN (SELECT SUBSCRIBER_NO, BAN FROM cdrdm.dim_mps_cust_1 WHERE MPS_CUST_SEQ_NO = 1 AND PRODUCT_TYPE = 'C') d ON (SUBSTR(trim(MMTelSplmtrySrvcInfo1Redir),INSTR(trim(MMTelSplmtrySrvcInfo1Redir),'|')+2) = trim(d.subscriber_no))
--Phase 2 changes starts
LEFT OUTER JOIN (SELECT SUBSCRIBER_NO, BAN, ACCOUNT_SUB_TYPE, PRODUCT_TYPE, COMPANY_CODE, FRANCHISE_CD, TR_BA_ACCOUNT_TYPE FROM cdrdm.dim_mps_cust_1 WHERE MPS_CUST_SEQ_NO = 1 AND PRODUCT_TYPE = 'U') e ON (SUBSTR(TRIM(SubscriberE164),INSTR(TRIM(SubscriberE164),'|')+2) = TRIM(e.subscriber_no))
LEFT OUTER JOIN (SELECT SUBSCRIBER_NO, BAN FROM cdrdm.dim_mps_cust_1 WHERE MPS_CUST_SEQ_NO = 1 AND PRODUCT_TYPE = 'U') f ON (SUBSTR(trim(OtherPartyAddress),INSTR(trim(OtherPartyAddress),'|')+2) = trim(f.subscriber_no))
LEFT OUTER JOIN (SELECT SUBSCRIBER_NO, BAN FROM cdrdm.dim_mps_cust_1 WHERE MPS_CUST_SEQ_NO = 1 AND PRODUCT_TYPE = 'U') g ON (SUBSTR(trim(MMTelSplmtrySrvcInfo1Redir),INSTR(trim(MMTelSplmtrySrvcInfo1Redir),'|')+2) = trim(g.subscriber_no))
--Phase 2 changes ends
;

-- PHASE 2 CHANGES - SOLUTION 2 ENDS FOR ucc1205ascii_temp2

-- Temp processing table...for product_type='C'
CREATE TABLE ext_cdrdm.ucc1205ascii_BAN AS
SELECT
TO_DATE(srvcrequestdttmnrml) AS srvcrequestdttmnrml,
SUB_NO_E164,
SUB_NO_PartyAddr,
SUB_NO_Tel,
BAN_TR_Sub_ALS_ind,
BAN_TR_Sub_oth_party_ALS_ind,
BAN_TR_Re_dir1_ALS_ind
FROM ext_cdrdm.ucc1205ascii_temp2
WHERE TR_PRODUCT_TYPE = 'C'
;

------------- TR_SUB_ALS_IND field -------------------
CREATE TABLE ext_cdrdm.tmp_SUB_ALS_IND_tbl AS 
SELECT SUB_NO_E164, BAN_TR_Sub_ALS_ind, ALS_IND, 1 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_E164,ucc.BAN_TR_Sub_ALS_ind, sub_hist.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_E164, ucc.BAN_TR_Sub_ALS_ind ORDER BY sub_hist.SYS_CREATION_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE
             FROM ELA_V21.subscriber_history
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub_hist
       ON ucc.SUB_NO_E164 = sub_hist.subscriber_no
       AND ucc.BAN_TR_Sub_ALS_ind = sub_hist.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml BETWEEN TO_DATE(sub_hist.EFFECTIVE_DATE) AND TO_DATE(sub_hist.EXPIRATION_DATE)
       AND sub_hist.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1

UNION ALL

SELECT SUB_NO_E164, BAN_TR_Sub_ALS_ind, ALS_IND, 2 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_E164,ucc.BAN_TR_Sub_ALS_ind, sub.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_E164, ucc.BAN_TR_Sub_ALS_ind ORDER BY sub.SYS_UPDATE_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, sub_status, EFFECTIVE_DATE, SYS_UPDATE_DATE
             FROM ELA_V21.subscriber
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub
       ON ucc.SUB_NO_E164 = sub.subscriber_no
       AND ucc.BAN_TR_Sub_ALS_ind = sub.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml >= TO_DATE(sub.EFFECTIVE_DATE)
       AND sub.sub_status = 'A'
       AND sub.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1
;

--------------- TR_SUB_OTH_PARTY_ALS_IND field -----------------
CREATE TABLE ext_cdrdm.tmp_SUB_OTH_PARTY_ALS_IND_tbl AS
SELECT SUB_NO_PartyAddr, BAN_TR_Sub_oth_party_ALS_ind, ALS_IND, 1 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_PartyAddr,ucc.BAN_TR_Sub_oth_party_ALS_ind, sub_hist.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_PartyAddr, ucc.BAN_TR_Sub_oth_party_ALS_ind ORDER BY sub_hist.SYS_CREATION_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE
             FROM ELA_V21.subscriber_history
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub_hist
       ON ucc.SUB_NO_PartyAddr = sub_hist.subscriber_no
       AND ucc.BAN_TR_Sub_oth_party_ALS_ind = sub_hist.customer_BAN -- ) Z
       WHERE ucc.SrvcRequestDttmNrml BETWEEN TO_DATE(sub_hist.EFFECTIVE_DATE) AND TO_DATE(sub_hist.EXPIRATION_DATE)
       AND sub_hist.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1

UNION ALL

SELECT SUB_NO_PartyAddr, BAN_TR_Sub_oth_party_ALS_ind, ALS_IND, 2 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_PartyAddr,ucc.BAN_TR_Sub_oth_party_ALS_ind, sub.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_PartyAddr, ucc.BAN_TR_Sub_oth_party_ALS_ind ORDER BY sub.SYS_UPDATE_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, sub_status, EFFECTIVE_DATE, SYS_UPDATE_DATE
             FROM ELA_V21.subscriber
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub
       ON ucc.SUB_NO_PartyAddr = sub.subscriber_no
       AND ucc.BAN_TR_Sub_oth_party_ALS_ind = sub.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml >= TO_DATE(sub.EFFECTIVE_DATE)
       AND sub.sub_status = 'A'
       AND sub.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1
;

--------------- TR_RE_DIR1_ALS_IND field -----------------
CREATE TABLE ext_cdrdm.tmp_TR_RE_DIR1_ALS_IND_tbl AS 
SELECT SUB_NO_Tel, BAN_TR_Re_dir1_ALS_ind, ALS_IND, 1 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_Tel,ucc.BAN_TR_Re_dir1_ALS_ind, sub_hist.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_Tel, ucc.BAN_TR_Re_dir1_ALS_ind ORDER BY sub_hist.SYS_CREATION_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE
             FROM ELA_V21.subscriber_history
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub_hist
       ON ucc.SUB_NO_Tel = sub_hist.subscriber_no
       AND ucc.BAN_TR_Re_dir1_ALS_ind = sub_hist.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml BETWEEN TO_DATE(sub_hist.EFFECTIVE_DATE) AND TO_DATE(sub_hist.EXPIRATION_DATE)
       AND sub_hist.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1

UNION ALL

SELECT SUB_NO_Tel, BAN_TR_Re_dir1_ALS_ind, ALS_IND, 2 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_Tel,ucc.BAN_TR_Re_dir1_ALS_ind, sub.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_Tel, ucc.BAN_TR_Re_dir1_ALS_ind ORDER BY sub.SYS_UPDATE_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, sub_status, EFFECTIVE_DATE, SYS_UPDATE_DATE
             FROM ELA_V21.subscriber
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub
       ON ucc.SUB_NO_Tel = sub.subscriber_no
       AND ucc.BAN_TR_Re_dir1_ALS_ind = sub.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml >= TO_DATE(sub.EFFECTIVE_DATE)
       AND sub.sub_status = 'A'
       AND sub.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1
;

--------------- TR_SUB_IND_GEN field -----------------

CREATE TABLE ext_cdrdm.tmp_TR_SUB_IND_GEN_tbl AS
SELECT SUB_NO_Tel, ALTERNATE_BAN
FROM (
       SELECT 
         ucc.SUB_NO_Tel, h.ALTERNATE_BAN, ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_Tel ORDER BY h.SYS_CREATION_DATE DESC) AS latest_ALTERNATE_BAN
       FROM ext_cdrdm.ucc1205ascii_BAN ucc
       JOIN ela_v21.als_hierarchy h
       ON ucc.SUB_NO_Tel = h.ALTERNATE_SUBSCRIBER
       WHERE ucc.SrvcRequestDttmNrml BETWEEN TO_DATE(h.EFFECTIVE_DATE) AND COALESCE(TO_DATE(h.EXPIRATION_DATE), '2099-12-31')
) X
WHERE latest_ALTERNATE_BAN = 1
;



-- PHASE 2 CHANGES - NEW TABLE STARTS
-- ALS_IND, SUB_OTH_PARTY_ALS_IND, RE_DIR1_ALS_IND
-- FOR PRODUCT TYPE = U

-- Temp processing table...for product_type='U'
CREATE TABLE ext_cdrdm.ucc1205ascii_WL_BAN AS
SELECT
TO_DATE(srvcrequestdttmnrml) AS srvcrequestdttmnrml,
SUB_NO_E164,
SUB_NO_PartyAddr,
SUB_NO_Tel,
BAN_WL_Sub_ALS_ind,
BAN_WL_Sub_oth_party_ALS_ind,
BAN_WL_Re_dir1_ALS_ind
FROM ext_cdrdm.ucc1205ascii_temp2
WHERE WL_PRODUCT_TYPE = 'U'
;

------------- WL_SUB_ALS_IND field -------------------
CREATE TABLE ext_cdrdm.tmp_WL_SUB_ALS_IND_tbl AS 
SELECT SUB_NO_E164, BAN_WL_Sub_ALS_ind, ALS_IND, 1 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_E164,ucc.BAN_WL_Sub_ALS_ind, sub_hist.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_E164, ucc.BAN_WL_Sub_ALS_ind ORDER BY sub_hist.SYS_CREATION_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_WL_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE
             FROM ELA_V21.subscriber_history
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub_hist
       ON ucc.SUB_NO_E164 = sub_hist.subscriber_no
       AND ucc.BAN_WL_Sub_ALS_ind = sub_hist.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml BETWEEN TO_DATE(sub_hist.EFFECTIVE_DATE) AND TO_DATE(sub_hist.EXPIRATION_DATE)
       AND sub_hist.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1

UNION ALL

SELECT SUB_NO_E164, BAN_WL_Sub_ALS_ind, ALS_IND, 2 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_E164,ucc.BAN_WL_Sub_ALS_ind, sub.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_E164, ucc.BAN_WL_Sub_ALS_ind ORDER BY sub.SYS_UPDATE_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_WL_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, sub_status, EFFECTIVE_DATE, SYS_UPDATE_DATE
             FROM ELA_V21.subscriber
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub
       ON ucc.SUB_NO_E164 = sub.subscriber_no
       AND ucc.BAN_WL_Sub_ALS_ind = sub.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml >= TO_DATE(sub.EFFECTIVE_DATE)
       AND sub.sub_status = 'A'
       AND sub.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1
;

--------------- WL_SUB_OTH_PARTY_ALS_IND field -----------------
CREATE TABLE ext_cdrdm.tmp_WL_SUB_OTH_PARTY_ALS_IND_tbl AS
SELECT SUB_NO_PartyAddr, BAN_WL_Sub_oth_party_ALS_ind, ALS_IND, 1 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_PartyAddr,ucc.BAN_WL_Sub_oth_party_ALS_ind, sub_hist.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_PartyAddr, ucc.BAN_WL_Sub_oth_party_ALS_ind ORDER BY sub_hist.SYS_CREATION_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_WL_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE
             FROM ELA_V21.subscriber_history
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub_hist
       ON ucc.SUB_NO_PartyAddr = sub_hist.subscriber_no
       AND ucc.BAN_WL_Sub_oth_party_ALS_ind = sub_hist.customer_BAN -- ) Z
       WHERE ucc.SrvcRequestDttmNrml BETWEEN TO_DATE(sub_hist.EFFECTIVE_DATE) AND TO_DATE(sub_hist.EXPIRATION_DATE)
       AND sub_hist.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1

UNION ALL

SELECT SUB_NO_PartyAddr, BAN_WL_Sub_oth_party_ALS_ind, ALS_IND, 2 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_PartyAddr,ucc.BAN_WL_Sub_oth_party_ALS_ind, sub.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_PartyAddr, ucc.BAN_WL_Sub_oth_party_ALS_ind ORDER BY sub.SYS_UPDATE_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_WL_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, sub_status, EFFECTIVE_DATE, SYS_UPDATE_DATE
             FROM ELA_V21.subscriber
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub
       ON ucc.SUB_NO_PartyAddr = sub.subscriber_no
       AND ucc.BAN_WL_Sub_oth_party_ALS_ind = sub.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml >= TO_DATE(sub.EFFECTIVE_DATE)
       AND sub.sub_status = 'A'
       AND sub.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1
;

--------------- WL_RE_DIR1_ALS_IND field -----------------
CREATE TABLE ext_cdrdm.tmp_WL_RE_DIR1_ALS_IND_tbl AS 
SELECT SUB_NO_Tel, BAN_WL_Re_dir1_ALS_ind, ALS_IND, 1 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_Tel,ucc.BAN_WL_Re_dir1_ALS_ind, sub_hist.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_Tel, ucc.BAN_WL_Re_dir1_ALS_ind ORDER BY sub_hist.SYS_CREATION_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_WL_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE
             FROM ELA_V21.subscriber_history
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub_hist
       ON ucc.SUB_NO_Tel = sub_hist.subscriber_no
       AND ucc.BAN_WL_Re_dir1_ALS_ind = sub_hist.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml BETWEEN TO_DATE(sub_hist.EFFECTIVE_DATE) AND TO_DATE(sub_hist.EXPIRATION_DATE)
       AND sub_hist.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1

UNION ALL

SELECT SUB_NO_Tel, BAN_WL_Re_dir1_ALS_ind, ALS_IND, 2 AS tbl_priority
FROM (
       SELECT ucc.SUB_NO_Tel,ucc.BAN_WL_Re_dir1_ALS_ind, sub.ALS_IND
            ,ROW_NUMBER() OVER(PARTITION BY ucc.SUB_NO_Tel, ucc.BAN_WL_Re_dir1_ALS_ind ORDER BY sub.SYS_UPDATE_DATE DESC) AS latest_ALS_IND
       FROM ext_cdrdm.ucc1205ascii_WL_BAN ucc
       JOIN (SELECT subscriber_no, customer_BAN, ALS_IND, sub_status, EFFECTIVE_DATE, SYS_UPDATE_DATE
             FROM ELA_V21.subscriber
             WHERE subscriber_no IS NOT NULL
             AND customer_BAN IS NOT NULL
             AND ALS_IND IN ('P','H','U','V')
       ) sub
       ON ucc.SUB_NO_Tel = sub.subscriber_no
       AND ucc.BAN_WL_Re_dir1_ALS_ind = sub.customer_BAN
       WHERE ucc.SrvcRequestDttmNrml >= TO_DATE(sub.EFFECTIVE_DATE)
       AND sub.sub_status = 'A'
       AND sub.ALS_IND IS NOT NULL
) Z
WHERE latest_ALS_IND=1
;

-- PHASE 2 CHANGES - NEW TABLE ENDS

--------------- Final JOIN -----------------
INSERT INTO TABLE cdrdm.FACT_UCC_CDR PARTITION (SrvcRequestDttmNrml_Date)
SELECT  
        ucc.recordtype,
        ucc.retransmission,
        ucc.sipmethod,
        ucc.roleofnode,
        ucc.nodeaddress,
        ucc.sessionid,
        ucc.callingpartyaddress_orig,
        ucc.callingpartyaddress,
        ucc.calledpartyaddress_orig,
        ucc.calledpartyaddress,
        ucc.srvcrequesttimestamp,
        ucc.srvcdeliverystarttimestamp,
        ucc.srvcdeliveryendtimestamp,
        ucc.recordopeningtime,
        ucc.recordclosuretime,
        ucc.srvcrequestdttmnrml,
        ucc.srvcdeliverystartdttmnrml,
        ucc.srvcdeliveryenddttmnrml,
        ucc.chargeableduration,
        ucc.tmfromsiprqststartofchrgng,
        ucc.interoperatoridentifiers,
        ucc.localrecordsequencenumber,
        ucc.partialoutputrecordnumber,
        ucc.causeforrecordclosing_orig,
        ucc.causecodepreestablishedsession,
        ucc.reasoncodepreestablishedsession,
        ucc.causecodeestablishedsession,
        ucc.reasoncodeestablishedsession,
        ucc.incomplete_cdr_ind_orig,
        ucc.acrstartlost,
        ucc.acrinterimlost,
        ucc.acrstoplost,
        ucc.imsilost,
        ucc.acrsccasstartlost,
        ucc.acrsccasinterimlost,
        ucc.acrsccasstoplost,
        ucc.imschargingidentifier,
        ucc.lstofsdpmediacomponents,
        ucc.lstofnrmlmediac1chgtime,
        ucc.lstofnrmlmediac1chgtimenor,
        ucc.lstofnrmlmediac1desctype1,
        ucc.lstofnrmlmediac1desccodec1,
        ucc.lstofnrmlmediac1descbndw1,
        ucc.lstofnrmlmediac1desctype2,
        ucc.lstofnrmlmediac1desccodec2,
        ucc.lstofnrmlmediac1descbndw2,
        ucc.lstofnrmlmediac1desctype3,
        ucc.lstofnrmlmediac1desccodec3,
        ucc.lstofnrmlmediac1descbndw3,
        ucc.lstofnrmlmediac1medinitflg,
        ucc.lstofnrmlmediac2chgtime,
        ucc.lstofnrmlmediac2chgtimenor,
        ucc.lstofnrmlmediac2desctype1,
        ucc.lstofnrmlmediac2desccodec1,
        ucc.lstofnrmlmediac2descbndw1,
        ucc.lstofnrmlmediac2desctype2,
        ucc.lstofnrmlmediac2desccodec2,
        ucc.lstofnrmlmediac2descbndw2,
        ucc.lstofnrmlmediac2desctype3,
        ucc.lstofnrmlmediac2desccodec3,
        ucc.lstofnrmlmediac2descbndw3,
        ucc.lstofnrmlmediac2medinitflg,
        ucc.lstofnrmlmediac3chgtime,
        ucc.lstofnrmlmediac3chgtimenor,
        ucc.lstofnrmlmediac3desctype1,
        ucc.lstofnrmlmediac3desccodec1,
        ucc.lstofnrmlmediac3descbndw1,
        ucc.lstofnrmlmediac3desctype2,
        ucc.lstofnrmlmediac3desccodec2,
        ucc.lstofnrmlmediac3descbndw2,
        ucc.lstofnrmlmediac3desctype3,
        ucc.lstofnrmlmediac3desccodec3,
        ucc.lstofnrmlmediac3descbndw3,
        ucc.lstofnrmlmediac3medinitflg,
        ucc.lstofnrmlmediac4chgtime,
        ucc.lstofnrmlmediac4chgtimenor,
        ucc.lstofnrmlmediac4desctype1,
        ucc.lstofnrmlmediac4desccodec1,
        ucc.lstofnrmlmediac4descbndw1,
        ucc.lstofnrmlmediac4desctype2,
        ucc.lstofnrmlmediac4desccodec2,
        ucc.lstofnrmlmediac4descbndw2,
        ucc.lstofnrmlmediac4desctype3,
        ucc.lstofnrmlmediac4desccodec3,
        ucc.lstofnrmlmediac4descbndw3,
        ucc.lstofnrmlmediac4medinitflg,
        ucc.lstofnrmlmediac5chgtime,
        ucc.lstofnrmlmediac5chgtimenor,
        ucc.lstofnrmlmediac5desctype1,
        ucc.lstofnrmlmediac5desccodec1,
        ucc.lstofnrmlmediac5descbndw1,
        ucc.lstofnrmlmediac5desctype2,
        ucc.lstofnrmlmediac5desccodec2,
        ucc.lstofnrmlmediac5descbndw2,
        ucc.lstofnrmlmediac5desctype3,
        ucc.lstofnrmlmediac5desccodec3,
        ucc.lstofnrmlmediac5descbndw3,
        ucc.lstofnrmlmediac5medinitflg,
        ucc.list_of_normalized_media_components6,
        ucc.list_of_normalized_media_components7,
        ucc.list_of_normalized_media_components8,
        ucc.list_of_normalized_media_components9,
        ucc.list_of_normalized_media_components10,
        ucc.list_of_normalized_media_components11,
        ucc.list_of_normalized_media_components12,
        ucc.list_of_normalized_media_components13,
        ucc.list_of_normalized_media_components14,
        ucc.list_of_normalized_media_components15,
        ucc.list_of_normalized_media_components16,
        ucc.list_of_normalized_media_components17,
        ucc.list_of_normalized_media_components18,
        ucc.list_of_normalized_media_components19,
        ucc.list_of_normalized_media_components20,
        ucc.list_of_normalized_media_components21,
        ucc.list_of_normalized_media_components22,
        ucc.list_of_normalized_media_components23,
        ucc.list_of_normalized_media_components24,
        ucc.list_of_normalized_media_components25,
        ucc.list_of_normalized_media_components26,
        ucc.list_of_normalized_media_components27,
        ucc.list_of_normalized_media_components28,
        ucc.list_of_normalized_media_components29,
        ucc.list_of_normalized_media_components30,
        ucc.list_of_normalized_media_components31,
        ucc.list_of_normalized_media_components32,
        ucc.list_of_normalized_media_components33,
        ucc.list_of_normalized_media_components34,
        ucc.list_of_normalized_media_components35,
        ucc.list_of_normalized_media_components36,
        ucc.list_of_normalized_media_components37,
        ucc.list_of_normalized_media_components38,
        ucc.list_of_normalized_media_components39,
        ucc.list_of_normalized_media_components40,
        ucc.list_of_normalized_media_components41,
        ucc.list_of_normalized_media_components42,
        ucc.list_of_normalized_media_components43,
        ucc.list_of_normalized_media_components44,
        ucc.list_of_normalized_media_components45,
        ucc.list_of_normalized_media_components46,
        ucc.list_of_normalized_media_components47,
        ucc.list_of_normalized_media_components48,
        ucc.list_of_normalized_media_components49,
        ucc.list_of_normalized_media_components50,
        ucc.listofnrmlmedcompts1150,
        ucc.ggsnaddress,
        ucc.servicereasonreturncode,
        ucc.lstofmessagebodies,
        ucc.recordextension,
        ucc.expires,
        ucc.event,
        ucc.lst1accessnetworkinfo,
        ucc.lst1accessdomain,
        ucc.lst1accesstype,
        ucc.lst1locationinfotype,
        ucc.lst1changetime,
        ucc.lst1changetimenormalized,
        ucc.lst2accessnetworkinfo,
        ucc.lst2accessdomain,
        ucc.lst2accesstype,
        ucc.lst2locationinfotype,
        ucc.lst2changetime,
        ucc.lst2changetimenormalized,
        ucc.lst3accessnetworkinfo,
        ucc.lst3accessdomain,
        ucc.lst3accesstype,
        ucc.lst3locationinfotype,
        ucc.lst3changetime,
        ucc.lst3changetimenormalized,
        ucc.lst4accessnetworkinfo,
        ucc.lst4accessdomain,
        ucc.lst4accesstype,
        ucc.lst4locationinfotype,
        ucc.lst4changetime,
        ucc.lst4changetimenormalized,
        ucc.lst5accessnetworkinfo,
        ucc.lst5accessdomain,
        ucc.lst5accesstype,
        ucc.lst5locationinfotype,
        ucc.lst5changetime,
        ucc.lst5changetimenormalized,
        ucc.lst6accessnetworkinfo,
        ucc.lst6accessdomain,
        ucc.lst6accesstype,
        ucc.lst6locationinfotype,
        ucc.lst6changetime,
        ucc.lst6changetimenormalized,
        ucc.lst7accessnetworkinfo,
        ucc.lst7accessdomain,
        ucc.lst7accesstype,
        ucc.lst7locationinfotype,
        ucc.lst7changetime,
        ucc.lst7changetimenormalized,
        ucc.lst8accessnetworkinfo,
        ucc.lst8accessdomain,
        ucc.lst8accesstype,
        ucc.lst8locationinfotype,
        ucc.lst8changetime,
        ucc.lst8changetimenormalized,
        ucc.lst9accessnetworkinfo,
        ucc.lst9accessdomain,
        ucc.lst9accesstype,
        ucc.lst9locationinfotype,
        ucc.lst9changetime,
        ucc.lst9changetimenormalized,
        ucc.lst10accessnetworkinfo,
        ucc.lst10accessdomain,
        ucc.lst10accesstype,
        ucc.lst10locationinfotype,
        ucc.lst10changetime,
        ucc.lst10changetimenormalized,
        ucc.servicecontextid,
--Phase 2 changes starts
        ucc.SubscriberE164_orig,
--Phase 2 changes ends          
        ucc.subscribere164,
        ucc.subscriberno,
        ucc.imsi,
        ucc.imei,
        ucc.subsipuri,
        ucc.nai,
        ucc.subprivate,
        ucc.subservedpartydeviceid,
--Phase 2 changes starts
        ucc.OtherPartyAddress_orig,
--Phase 2 changes ends          
        ucc.otherpartyaddress,
        ucc.phonetype,
        ucc.serviceprovider,
        ucc.hg_group,
--Phase 2 changes starts
        ucc.End_User_SubsID_orig,
        ucc.End_User_SubsID_CTN,
        ucc.End_User_SubsID_Dom_fl,
        ucc.End_User_SubsID_Dom_ty,
--Phase 2 changes ends                  
        ucc.lstofearlysdpmediacmpnts,
        ucc.imscommserviceidentifier,
        ucc.numberportabilityrouting,
        ucc.carrierselectrouting,
        ucc.sessionpriority,
--Phase 2 changes starts
        ucc.RequestedPartyAddress_orig,
--Phase 2 changes ends                          
        ucc.requestedpartyaddress,
        ucc.lstofcalledassertedid,
        ucc.mmtelinfoanalyzedcalltype,
        ucc.mtlinfocalledasrtidprsntsts,
        ucc.mtlinfocllgprtyaddrprsntsts,
        ucc.mmtelinfoconferenceid,
        ucc.mmtelinfodialaroundindctr,
        ucc.mmtelinforelatedicid,
        ucc.mmtelsplmtrysrvcinfo1id,
        ucc.mmtelsplmtrysrvcinfo1act,
--Phase 2 changes starts
        ucc.MMTelSplmtrySrvcInfo1Redir_orig,
--Phase 2 changes ends          
        ucc.mmtelsplmtrysrvcinfo1redir,
        ucc.mmtelsplmtrysrvcinfo2id,
        ucc.mmtelsplmtrysrvcinfo2act,
--Phase 2 changes starts
        ucc.MMTelSplmtrySrvcInfo2Redir_orig,
--Phase 2 changes ends                  
        ucc.mmtelsplmtrysrvcinfo2redir,
        ucc.mmtelsplmtrysrvcinfo3id,
        ucc.mmtelsplmtrysrvcinfo3act,
--Phase 2 changes starts
        ucc.MMTelSplmtrySrvcInfo3Redir_orig,
--Phase 2 changes ends                  
        ucc.mmtelsplmtrysrvcinfo3redir,
        ucc.mmtelsplmtrysrvcinfo4id,
        ucc.mmtelsplmtrysrvcinfo4act,
--Phase 2 changes starts
        ucc.MMTelSplmtrySrvcInfo4Redir_orig,
--Phase 2 changes ends                  
        ucc.mmtelsplmtrysrvcinfo4redir,
        ucc.mmtelsplmtrysrvcinfo5id,
        ucc.mmtelsplmtrysrvcinfo5act,
--Phase 2 changes starts
        ucc.MMTelSplmtrySrvcInfo5Redir_orig,
--Phase 2 changes ends          
        ucc.mmtelsplmtrysrvcinfo5redir,
        ucc.mmtelinformation_supplementary_service_information6,
        ucc.mmtelinformation_supplementary_service_information7,
        ucc.mmtelinformation_supplementary_service_information8,
        ucc.mmtelinformation_supplementary_service_information9,
        ucc.mmtelinformation_supplementary_service_information10,
        ucc.mobilestationroamingnumber,
        ucc.teleservicecode,
        ucc.tariffclass,
        ucc.pvisitednetworkid,
--Phase 2 changes starts
        ucc.networkCallType,
        ucc.carrierIdCode,
--Phase 2 changes ends          
-----------        
        ucc.tr_ban,
        ucc.tr_account_sub_type,
        ucc.tr_company_code,
        ucc.tr_franchise_cd,
--Phase 2 changes starts
        ucc.tr_product_type,
--Phase 2 changes ends
        ucc.tr_ba_account_type,
-----------
        tmp_ALS.ALS_IND AS TR_SUB_ALS_IND,
        tmp_PARTY.ALS_IND AS TR_SUB_OTH_PARTY_ALS_IND,
        tmp_DIR.ALS_IND AS TR_RE_DIR1_ALS_IND,
        tmp_GEN.ALTERNATE_BAN AS TR_SUB_IND_GEN,
-----------
--Phase 2 changes starts
        ucc.WL_BAN,
        ucc.WL_ACCOUNT_SUB_TYPE,
        ucc.WL_COMPANY_CODE,
        ucc.WL_FRANCHISE_CD,
        ucc.WL_PRODUCT_TYPE,
        ucc.WL_BA_ACCOUNT_TYPE,      
-----------
        tmp_WL_ALS.ALS_IND AS WL_SUB_ALS_IND,
        tmp_WL_PARTY.ALS_IND AS WL_SUB_OTH_PARTY_ALS_IND,
        tmp_WL_DIR.ALS_IND AS WL_RE_DIR1_ALS_IND,
--Phase 2 changes ends
-----------
        ucc.file_name,
        ucc.record_start,
        ucc.record_end,
        ucc.record_type,
        ucc.family_name,
        ucc.version_id,
        ucc.file_time,
        ucc.file_id,
        ucc.switch_name,
        ucc.num_records,
        from_unixtime(unix_timestamp()) AS insert_timestamp,
       TO_DATE(SrvcRequestDttmNrml) AS SrvcRequestDttmNrml_Date 
FROM ext_cdrdm.ucc1205ascii_temp2 ucc
-- for TR_SUB_ALS_IND field
LEFT OUTER JOIN (SELECT SUB_NO_E164, BAN_TR_Sub_ALS_ind, ALS_IND 
                 FROM (SELECT SUB_NO_E164, BAN_TR_Sub_ALS_ind, ALS_IND, ROW_NUMBER() OVER(PARTITION BY SUB_NO_E164, BAN_TR_Sub_ALS_ind ORDER BY tbl_priority ASC) AS prirt
                       FROM ext_cdrdm.tmp_SUB_ALS_IND_tbl  -- ext_cdrdm.tmp_ALS_IND_tbl
                       ) tmp_1
                 WHERE prirt=1
) tmp_ALS
-- "ON SUBSTR(TRIM(ucc.SubscriberE164),INSTR(TRIM(ucc.SubscriberE164),'|')+2) = tmp_ALS.subscriber_no"
ON ucc.SUB_NO_E164 = tmp_ALS.SUB_NO_E164
AND ucc.BAN_TR_Sub_ALS_ind = tmp_ALS.BAN_TR_Sub_ALS_ind

-- for TR_SUB_OTH_PARTY_ALS_IND
LEFT OUTER JOIN (SELECT SUB_NO_PartyAddr,BAN_TR_Sub_oth_party_ALS_ind, ALS_IND 
                 FROM (SELECT SUB_NO_PartyAddr,BAN_TR_Sub_oth_party_ALS_ind, ALS_IND, ROW_NUMBER() OVER(PARTITION BY SUB_NO_PartyAddr,BAN_TR_Sub_oth_party_ALS_ind ORDER BY tbl_priority ASC) AS prirt
                       FROM ext_cdrdm.tmp_SUB_OTH_PARTY_ALS_IND_tbl
                       ) tmp_2
                 WHERE prirt=1
) tmp_PARTY
-- "ON SUBSTR(TRIM(ucc.OtherPartyAddress),INSTR(TRIM(ucc.OtherPartyAddress),'|')+2) = tmp_PARTY.subscriber_no"
ON ucc.SUB_NO_PartyAddr = tmp_PARTY.SUB_NO_PartyAddr
AND ucc.BAN_TR_Sub_oth_party_ALS_ind = tmp_PARTY.BAN_TR_Sub_oth_party_ALS_ind

-- TR_RE_DIR1_ALS_IND
LEFT OUTER JOIN (SELECT SUB_NO_Tel, BAN_TR_Re_dir1_ALS_ind, ALS_IND 
                 FROM (SELECT SUB_NO_Tel, BAN_TR_Re_dir1_ALS_ind, ALS_IND, ROW_NUMBER() OVER(PARTITION BY SUB_NO_Tel, BAN_TR_Re_dir1_ALS_ind ORDER BY tbl_priority ASC) AS prirt
                       FROM ext_cdrdm.tmp_TR_RE_DIR1_ALS_IND_tbl
                       ) tmp_3
                 WHERE prirt=1
) tmp_DIR
-- "ON SUBSTR(TRIM(ucc.MMTelSplmtrySrvcInfo1Redir),INSTR(TRIM(ucc.MMTelSplmtrySrvcInfo1Redir),'|')+2) = tmp_DIR.subscriber_no"
ON ucc.SUB_NO_Tel = tmp_DIR.SUB_NO_Tel
AND ucc.BAN_TR_Re_dir1_ALS_ind = tmp_DIR.BAN_TR_Re_dir1_ALS_ind

-- TR_SUB_IND_GEN
LEFT OUTER JOIN (SELECT SUB_NO_Tel, ALTERNATE_BAN
                 FROM ext_cdrdm.tmp_TR_SUB_IND_GEN_tbl 
                 ) tmp_GEN
ON ucc.SUB_NO_Tel = tmp_GEN.SUB_NO_Tel

-- PHASE 2 CHANGES - STARTS

-- for WL_SUB_ALS_IND field
LEFT OUTER JOIN (SELECT SUB_NO_E164, BAN_WL_Sub_ALS_ind, ALS_IND 
                 FROM (SELECT SUB_NO_E164, BAN_WL_Sub_ALS_ind, ALS_IND, ROW_NUMBER() OVER(PARTITION BY SUB_NO_E164, BAN_WL_Sub_ALS_ind ORDER BY tbl_priority ASC) AS prirt
                       FROM ext_cdrdm.tmp_WL_SUB_ALS_IND_tbl  -- ext_cdrdm.tmp_ALS_IND_tbl
                       ) tmp_1
                 WHERE prirt=1
) tmp_WL_ALS
-- "ON SUBSTR(TRIM(ucc.SubscriberE164),INSTR(TRIM(ucc.SubscriberE164),'|')+2) = tmp_ALS.subscriber_no"
ON ucc.SUB_NO_E164 = tmp_ALS.SUB_NO_E164
AND ucc.BAN_WL_Sub_ALS_ind = tmp_WL_ALS.BAN_WL_Sub_ALS_ind

-- for WL_SUB_OTH_PARTY_ALS_IND
LEFT OUTER JOIN (SELECT SUB_NO_PartyAddr,BAN_WL_Sub_oth_party_ALS_ind, ALS_IND 
                 FROM (SELECT SUB_NO_PartyAddr,BAN_WL_Sub_oth_party_ALS_ind, ALS_IND, ROW_NUMBER() OVER(PARTITION BY SUB_NO_PartyAddr,BAN_WL_Sub_oth_party_ALS_ind ORDER BY tbl_priority ASC) AS prirt
                       FROM ext_cdrdm.tmp_WL_SUB_OTH_PARTY_ALS_IND_tbl
                       ) tmp_2
                 WHERE prirt=1
) tmp_WL_PARTY
-- "ON SUBSTR(TRIM(ucc.OtherPartyAddress),INSTR(TRIM(ucc.OtherPartyAddress),'|')+2) = tmp_PARTY.subscriber_no"
ON ucc.SUB_NO_PartyAddr = tmp_PARTY.SUB_NO_PartyAddr
AND ucc.BAN_WL_Sub_oth_party_ALS_ind = tmp_WL_PARTY.BAN_WL_Sub_oth_party_ALS_ind

-- for WL_RE_DIR1_ALS_IND
LEFT OUTER JOIN (SELECT SUB_NO_Tel, BAN_WL_Re_dir1_ALS_ind, ALS_IND 
                 FROM (SELECT SUB_NO_Tel, BAN_WL_Re_dir1_ALS_ind, ALS_IND, ROW_NUMBER() OVER(PARTITION BY SUB_NO_Tel, BAN_WL_Re_dir1_ALS_ind ORDER BY tbl_priority ASC) AS prirt
                       FROM ext_cdrdm.tmp_WL_RE_DIR1_ALS_IND_tbl
                       ) tmp_3
                 WHERE prirt=1
) tmp_WL_DIR
-- "ON SUBSTR(TRIM(ucc.MMTelSplmtrySrvcInfo1Redir),INSTR(TRIM(ucc.MMTelSplmtrySrvcInfo1Redir),'|')+2) = tmp_DIR.subscriber_no"
ON ucc.SUB_NO_Tel = tmp_DIR.SUB_NO_Tel
AND ucc.BAN_WL_Re_dir1_ALS_ind = tmp_WL_DIR.BAN_WL_Re_dir1_ALS_ind 
-- PHASE 2 CHANGES - ENDS
;


