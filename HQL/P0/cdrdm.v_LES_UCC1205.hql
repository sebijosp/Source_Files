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

USE cdrdm;

DROP VIEW IF EXISTS v_LES_UCC1205;

CREATE VIEW IF NOT EXISTS v_LES_UCC1205 AS
SELECT 
a.IsWifiUsage,
a.RecordType
,a.Retransmission
,a.SIPMethod
,a.RoleOfNode
,a.NodeAddress
,a.SessionId
,a.CallingPartyAddress_orig 
,a.CallingPartyAddress
,a.CalledPartyAddress_orig 
,a.CalledPartyAddress
,a.SrvcRequestTimeStamp
,a.SrvcDeliveryStartTimeStamp
,a.SrvcDeliveryEndTimeStamp
,a.RecordOpeningTime
,a.RecordClosureTime
,a.SrvcRequestDttmNrml
,a.SrvcDeliveryStartDttmNrml
,a.SrvcDeliveryEndDttmNrml
,a.ChargeableDuration
,a.TmFromSipRqstStartOfChrgng
,a.InterOperatorIdentifiers
,a.LocalRecordSequenceNumber
,a.PartialOutputRecordNumber
,a.CauseForRecordClosing_Orig
,a.causeCodePreEstablishedSession  
,a.reasonCodePreEstablishedSession
,a.causeCodeEstablishedSession
,a.reasonCodeEstablishedSession
,a.incomplete_CDR_Ind_orig
,a.ACRStartLost
,a.ACRInterimLost
,a.ACRStopLost
,a.IMSILost
,a.ACRSCCASStartLost
,a.ACRSCCASInterimLost
,a.ACRSCCASStopLost
,a.IMSChargingIdentifier
,a.LstOfSDPMediaComponents
,a.LstOfNrmlMediaC1ChgTime
,a.LstOfNrmlMediaC1ChgTimeNor
,a.LstOfNrmlMediaC1DescType1
,a.LstOfNrmlMediaC1DescCodec1
,a.LstOfNrmlMediaC1DescBndW1
,a.LstOfNrmlMediaC1DescType2
,a.LstOfNrmlMediaC1DescCodec2
,a.LstOfNrmlMediaC1DescBndW2
,a.LstOfNrmlMediaC1DescType3
,a.LstOfNrmlMediaC1DescCodec3
,a.LstOfNrmlMediaC1DescBndW3
,a.LstOfNrmlMediaC1medInitFlg
,a.LstOfNrmlMediaC2ChgTime
,a.LstOfNrmlMediaC2ChgTimeNor
,a.LstOfNrmlMediaC2DescType1
,a.LstOfNrmlMediaC2DescCodec1
,a.LstOfNrmlMediaC2DescBndW1
,a.LstOfNrmlMediaC2DescType2
,a.LstOfNrmlMediaC2DescCodec2
,a.LstOfNrmlMediaC2DescBndW2
,a.LstOfNrmlMediaC2DescType3
,a.LstOfNrmlMediaC2DescCodec3
,a.LstOfNrmlMediaC2DescBndW3
,a.LstOfNrmlMediaC2medInitFlg
,a.LstOfNrmlMediaC3ChgTime
,a.LstOfNrmlMediaC3ChgTimeNor
,a.LstOfNrmlMediaC3DescType1
,a.LstOfNrmlMediaC3DescCodec1
,a.LstOfNrmlMediaC3DescBndW1
,a.LstOfNrmlMediaC3DescType2
,a.LstOfNrmlMediaC3DescCodec2
,a.LstOfNrmlMediaC3DescBndW2
,a.LstOfNrmlMediaC3DescType3
,a.LstOfNrmlMediaC3DescCodec3
,a.LstOfNrmlMediaC3DescBndW3
,a.LstOfNrmlMediaC3medInitFlg
,a.LstOfNrmlMediaC4ChgTime
,a.LstOfNrmlMediaC4ChgTimeNor
,a.LstOfNrmlMediaC4DescType1
,a.LstOfNrmlMediaC4DescCodec1
,a.LstOfNrmlMediaC4DescBndW1
,a.LstOfNrmlMediaC4DescType2
,a.LstOfNrmlMediaC4DescCodec2
,a.LstOfNrmlMediaC4DescBndW2
,a.LstOfNrmlMediaC4DescType3
,a.LstOfNrmlMediaC4DescCodec3
,a.LstOfNrmlMediaC4DescBndW3
,a.LstOfNrmlMediaC4medInitFlg
,a.LstOfNrmlMediaC5ChgTime
,a.LstOfNrmlMediaC5ChgTimeNor
,a.LstOfNrmlMediaC5DescType1
,a.LstOfNrmlMediaC5DescCodec1
,a.LstOfNrmlMediaC5DescBndW1
,a.LstOfNrmlMediaC5DescType2
,a.LstOfNrmlMediaC5DescCodec2
,a.LstOfNrmlMediaC5DescBndW2
,a.LstOfNrmlMediaC5DescType3
,a.LstOfNrmlMediaC5DescCodec3
,a.LstOfNrmlMediaC5DescBndW3
,a.LstOfNrmlMediaC5medInitFlg
,a.LstOfNrmlMediaComponents6
,a.LstOfNrmlMediaComponents7
,a.LstOfNrmlMediaComponents8
,a.LstOfNrmlMediaComponents9
,a.LstOfNrmlMediaComponents10
,a.LstOfNrmlMediaComponents11
,a.LstOfNrmlMediaComponents12
,a.LstOfNrmlMediaComponents13
,a.LstOfNrmlMediaComponents14
,a.LstOfNrmlMediaComponents15
,a.LstOfNrmlMediaComponents16
,a.LstOfNrmlMediaComponents17
,a.LstOfNrmlMediaComponents18
,a.LstOfNrmlMediaComponents19
,a.LstOfNrmlMediaComponents20
,a.LstOfNrmlMediaComponents21
,a.LstOfNrmlMediaComponents22
,a.LstOfNrmlMediaComponents23
,a.LstOfNrmlMediaComponents24
,a.LstOfNrmlMediaComponents25
,a.LstOfNrmlMediaComponents26
,a.LstOfNrmlMediaComponents27
,a.LstOfNrmlMediaComponents28
,a.LstOfNrmlMediaComponents29
,a.LstOfNrmlMediaComponents30
,a.LstOfNrmlMediaComponents31
,a.LstOfNrmlMediaComponents32
,a.LstOfNrmlMediaComponents33
,a.LstOfNrmlMediaComponents34
,a.LstOfNrmlMediaComponents35
,a.LstOfNrmlMediaComponents36
,a.LstOfNrmlMediaComponents37
,a.LstOfNrmlMediaComponents38
,a.LstOfNrmlMediaComponents39
,a.LstOfNrmlMediaComponents40
,a.LstOfNrmlMediaComponents41
,a.LstOfNrmlMediaComponents42
,a.LstOfNrmlMediaComponents43
,a.LstOfNrmlMediaComponents44
,a.LstOfNrmlMediaComponents45
,a.LstOfNrmlMediaComponents46
,a.LstOfNrmlMediaComponents47
,a.LstOfNrmlMediaComponents48
,a.LstOfNrmlMediaComponents49
,a.LstOfNrmlMediaComponents50
,a.LstOfNrmlMedCompts1150
,a.GGSNAddress
,a.ServiceReasonReturnCode
,a.LstOfMessageBodies
,a.RecordExtension
,a.Expires
,a.Event
,a.Lst1AccessNetworkInfo
,a.Lst1AccessDomain
,a.Lst1AccessType
,a.Lst1LocationInfoType
,a.Lst1ChangeTime
,a.Lst1ChangeTimeNormalized
--,B.FILE_NAME AS Lst1FILE_NAME
,B.SITE AS Lst1SITE
,B.CELL AS Lst1CELL
,B.ENODEB AS Lst1ENODEB
,B.CGI AS Lst1CGI
,B.CGI_HEX AS Lst1CGI_HEX
,B.SITE_NAME AS Lst1SITE_NAME
,B.ORIGINAL_I AS Lst1ORIGINAL_I
,B.ANTENNA_TY AS Lst1ANTENNA_TY
,B.AZIMUTH AS Lst1AZIMUTH
,B.BEAMWIDTH AS Lst1BEAMWIDTH
,B.X AS Lst1X
,B.Y AS Lst1Y
,B.LONGITUDE AS Lst1LONGITUDE
,B.LATITUDE AS Lst1LATITUDE
,B.ADDRESS AS Lst1ADDRESS
,B.CITY AS Lst1CITY
,B.PROVINCE AS Lst1PROVINCE
,B.ARFCNDL AS Lst1ARFCNDL
,B.BCCHNO AS Lst1BCCHNO
,B.LOCATIONCO AS Lst1LOCATIONCO
,B.BSIC AS Lst1BSIC
,B.PRIMARYSCR AS Lst1PRIMARYSCR
--,B.START_DATE AS Lst1START_DATE
--,B.END_DATE AS Lst1END_DATE
,B.INSERT_TIMESTAMP AS Lst1INSERT_TIMESTAMP
--,B.UPDATE_TIMESTAMP AS Lst1UPDATE_TIMESTAMP
--,B.WORKFLOW_RUN_ID AS Lst1WORKFLOW_RUN_ID
,a.Last_Cell
--,C.FILE_NAME AS Last_Cell_FILE_NAME
,C.SITE AS Last_Cell_SITE
,C.CELL AS Last_Cell_CELL
,C.ENODEB AS Last_Cell_ENODEB
,C.CGI AS Last_Cell_CGI
,C.CGI_HEX AS Last_Cell_CGI_HEX
,C.SITE_NAME AS Last_Cell_SITE_NAME
,C.ORIGINAL_I AS Last_Cell_ORIGINAL_I
,C.ANTENNA_TY AS Last_Cell_ANTENNA_TY
,C.AZIMUTH AS Last_Cell_AZIMUTH
,C.BEAMWIDTH AS Last_Cell_BEAMWIDTH
,C.X AS Last_Cell_X
,C.Y AS Last_Cell_Y
,C.LONGITUDE AS Last_Cell_LONGITUDE
,C.LATITUDE AS Last_Cell_LATITUDE
,C.ADDRESS AS Last_Cell_ADDRESS
,C.CITY AS Last_Cell_CITY
,C.PROVINCE AS Last_Cell_PROVINCE
,C.ARFCNDL AS Last_Cell_ARFCNDL
,C.BCCHNO AS Last_Cell_BCCHNO
,C.LOCATIONCO AS Last_Cell_LOCATIONCO
,C.BSIC AS Last_Cell_BSIC
,C.PRIMARYSCR AS Last_Cell_PRIMARYSCR
--,C.START_DATE AS Last_Cell_START_DATE
--,C.END_DATE AS Last_Cell_END_DATE
,C.INSERT_TIMESTAMP AS Last_Cell_INSERT_TIMESTAMP
--,C.UPDATE_TIMESTAMP AS Last_Cell_UPDATE_TIMESTAMP
--,C.WORKFLOW_RUN_ID AS Last_Cell_WORKFLOW_RUN_ID
,a.Lst2AccessNetworkInfo
,a.Lst2AccessDomain
,a.Lst2AccessType
,a.Lst2LocationInfoType
,a.Lst2ChangeTime
,a.Lst2ChangeTimeNormalized
,a.Lst3AccessNetworkInfo
,a.Lst3AccessDomain
,a.Lst3AccessType
,a.Lst3LocationInfoType
,a.Lst3ChangeTime
,a.Lst3ChangeTimeNormalized
,a.Lst4AccessNetworkInfo
,a.Lst4AccessDomain
,a.Lst4AccessType
,a.Lst4LocationInfoType
,a.Lst4ChangeTime
,a.Lst4ChangeTimeNormalized
,a.Lst5AccessNetworkInfo
,a.Lst5AccessDomain
,a.Lst5AccessType
,a.Lst5LocationInfoType
,a.Lst5ChangeTime
,a.Lst5ChangeTimeNormalized
,a.Lst6AccessNetworkInfo
,a.Lst6AccessDomain
,a.Lst6AccessType
,a.Lst6LocationInfoType
,a.Lst6ChangeTime
,a.Lst6ChangeTimeNormalized
,a.Lst7AccessNetworkInfo
,a.Lst7AccessDomain
,a.Lst7AccessType
,a.Lst7LocationInfoType
,a.Lst7ChangeTime
,a.Lst7ChangeTimeNormalized
,a.Lst8AccessNetworkInfo
,a.Lst8AccessDomain
,a.Lst8AccessType
,a.Lst8LocationInfoType
,a.Lst8ChangeTime
,a.Lst8ChangeTimeNormalized
,a.Lst9AccessNetworkInfo
,a.Lst9AccessDomain
,a.Lst9AccessType
,a.Lst9LocationInfoType
,a.Lst9ChangeTime
,a.Lst9ChangeTimeNormalized
,a.Lst10AccessNetworkInfo
,a.Lst10AccessDomain
,a.Lst10AccessType
,a.Lst10LocationInfoType
,a.Lst10ChangeTime
,a.Lst10ChangeTimeNormalized
,a.ServiceContextID
--phase 2 changes starts
,a.SubscriberE164_orig
--phase 2 changes ends
,a.SubscriberE164
,a.SubscriberNo
,a.IMSI
,a.IMEI
,a.SubSIPURI
,a.NAI
,a.SubPrivate
,a.SubServedPartyDeviceID
--phase 2 changes starts
,a.OtherPartyAddress_orig
--phase 2 changes ends
,a.OtherPartyAddress
,a.PhoneType
,a.ServiceProvider
,a.HG_Group
--phase 2 changes starts
,a.End_User_SubsID_orig
,a.End_User_SubsID_CTN
,a.End_User_SubsID_Dom_fl
,a.End_User_SubsID_Dom_ty
--phase 2 changes ends
,a.LstOfEarlySDPMediaCmpnts
,a.IMSCommServiceIdentifier
,a.NumberPortabilityRouting
,a.CarrierSelectRouting
,a.SessionPriority
--phase 2 changes starts
,a.RequestedPartyAddress_orig
--phase 2 changes ends
,a.RequestedPartyAddress
,a.LstOfCalledAssertedID
,a.MMTelInfoAnalyzedCallType
,a.MTlInfoCalledAsrtIDPrsntSts
,a.MTlInfoCllgPrtyAddrPrsntSts
,a.MMTelInfoConferenceId
,a.MMTelInfoDialAroundIndctr
,a.MMTelInfoRelatedICID
,a.MMTelSplmtrySrvcInfo1ID
,a.MMTelSplmtrySrvcInfo1Act
--phase 2 changes starts
,a.MMTelSplmtrySrvcInfo1Redir_orig
--phase 2 changes ends
,a.MMTelSplmtrySrvcInfo1Redir
,a.MMTelSplmtrySrvcInfo2ID
,a.MMTelSplmtrySrvcInfo2Act
--phase 2 changes starts
,a.MMTelSplmtrySrvcInfo2Redir_orig
--phase 2 changes ends
,a.MMTelSplmtrySrvcInfo2Redir
,a.MMTelSplmtrySrvcInfo3ID
,a.MMTelSplmtrySrvcInfo3Act
--phase 2 changes starts
,a.MMTelSplmtrySrvcInfo3Redir_orig
--phase 2 changes ends
,a.MMTelSplmtrySrvcInfo3Redir
,a.MMTelSplmtrySrvcInfo4ID
,a.MMTelSplmtrySrvcInfo4Act
--phase 2 changes starts
,a.MMTelSplmtrySrvcInfo4Redir_orig
--phase 2 changes ends
,a.MMTelSplmtrySrvcInfo4Redir
,a.MMTelSplmtrySrvcInfo5ID
,a.MMTelSplmtrySrvcInfo5Act
--phase 2 changes starts
,a.MMTelSplmtrySrvcInfo5Redir_orig
--phase 2 changes ends
,a.MMTelSplmtrySrvcInfo5Redir
,a.MMTelInfoSplmtrySrvcInfo6
,a.MMTelInfoSplmtrySrvcInfo7
,a.MMTelInfoSplmtrySrvcInfo8
,a.MMTelInfoSplmtrySrvcInfo9
,a.MMTelInfoSplmtrySrvcInfo10
,a.MobileStationRoamingNumber
,a.TeleServiceCode
,a.TariffClass
,a.pVisitedNetworkID
--phase 2 changes starts
,a.networkCallType
,a.carrierIdCode
--phase 2 changes ends
---- Add control fields information
,a.file_name
,a.record_start
,a.record_end
,a.record_type
,a.family_name
,a.version_id
,a.file_time
,a.file_id
,a.switch_name
,a.num_records
,a.insert_timestamp
,a.SrvcRequestDttmNrml_Date
FROM
(SELECT
IF((Case 
  when  Lst10AccessDomain = '20' then Lst10AccessDomain
  when  Lst9AccessDomain = '20' then Lst9AccessDomain
  when  Lst9AccessDomain = '20' then Lst8AccessDomain
  when  Lst7AccessDomain = '20' then Lst7AccessDomain
  when  Lst6AccessDomain = '20' then Lst6AccessDomain
  when  Lst5AccessDomain = '20' then Lst5AccessDomain
  when  Lst4AccessDomain = '20' then Lst4AccessDomain
  when  Lst3AccessDomain = '20' then Lst3AccessDomain
  when  Lst2AccessDomain = '20' then Lst2AccessDomain
  when  Lst1AccessDomain = '20' then Lst1AccessDomain
else Null
End) = '20','YES','NO') as IsWifiUsage, 
RecordType
,Retransmission
,SIPMethod
,RoleOfNode
,NodeAddress
,SessionId
,CallingPartyAddress_orig
,CallingPartyAddress
,CalledPartyAddress_orig
,CalledPartyAddress
,SrvcRequestTimeStamp
,SrvcDeliveryStartTimeStamp
,SrvcDeliveryEndTimeStamp
,RecordOpeningTime
,RecordClosureTime
,SrvcRequestDttmNrml
,SrvcDeliveryStartDttmNrml
,SrvcDeliveryEndDttmNrml
,ChargeableDuration
,TmFromSipRqstStartOfChrgng
,InterOperatorIdentifiers
,LocalRecordSequenceNumber
,PartialOutputRecordNumber
,CauseForRecordClosing_Orig
,causeCodePreEstablishedSession  
,reasonCodePreEstablishedSession
,causeCodeEstablishedSession
,reasonCodeEstablishedSession
,incomplete_CDR_Ind_orig
,ACRStartLost
,ACRInterimLost
,ACRStopLost
,IMSILost
,ACRSCCASStartLost
,ACRSCCASInterimLost
,ACRSCCASStopLost
,IMSChargingIdentifier
,LstOfSDPMediaComponents
,LstOfNrmlMediaC1ChgTime
,LstOfNrmlMediaC1ChgTimeNor
,LstOfNrmlMediaC1DescType1
,LstOfNrmlMediaC1DescCodec1
,LstOfNrmlMediaC1DescBndW1
,LstOfNrmlMediaC1DescType2
,LstOfNrmlMediaC1DescCodec2
,LstOfNrmlMediaC1DescBndW2
,LstOfNrmlMediaC1DescType3
,LstOfNrmlMediaC1DescCodec3
,LstOfNrmlMediaC1DescBndW3
,LstOfNrmlMediaC1medInitFlg
,LstOfNrmlMediaC2ChgTime
,LstOfNrmlMediaC2ChgTimeNor
,LstOfNrmlMediaC2DescType1
,LstOfNrmlMediaC2DescCodec1
,LstOfNrmlMediaC2DescBndW1
,LstOfNrmlMediaC2DescType2
,LstOfNrmlMediaC2DescCodec2
,LstOfNrmlMediaC2DescBndW2
,LstOfNrmlMediaC2DescType3
,LstOfNrmlMediaC2DescCodec3
,LstOfNrmlMediaC2DescBndW3
,LstOfNrmlMediaC2medInitFlg
,LstOfNrmlMediaC3ChgTime
,LstOfNrmlMediaC3ChgTimeNor
,LstOfNrmlMediaC3DescType1
,LstOfNrmlMediaC3DescCodec1
,LstOfNrmlMediaC3DescBndW1
,LstOfNrmlMediaC3DescType2
,LstOfNrmlMediaC3DescCodec2
,LstOfNrmlMediaC3DescBndW2
,LstOfNrmlMediaC3DescType3
,LstOfNrmlMediaC3DescCodec3
,LstOfNrmlMediaC3DescBndW3
,LstOfNrmlMediaC3medInitFlg
,LstOfNrmlMediaC4ChgTime
,LstOfNrmlMediaC4ChgTimeNor
,LstOfNrmlMediaC4DescType1
,LstOfNrmlMediaC4DescCodec1
,LstOfNrmlMediaC4DescBndW1
,LstOfNrmlMediaC4DescType2
,LstOfNrmlMediaC4DescCodec2
,LstOfNrmlMediaC4DescBndW2
,LstOfNrmlMediaC4DescType3
,LstOfNrmlMediaC4DescCodec3
,LstOfNrmlMediaC4DescBndW3
,LstOfNrmlMediaC4medInitFlg
,LstOfNrmlMediaC5ChgTime
,LstOfNrmlMediaC5ChgTimeNor
,LstOfNrmlMediaC5DescType1
,LstOfNrmlMediaC5DescCodec1
,LstOfNrmlMediaC5DescBndW1
,LstOfNrmlMediaC5DescType2
,LstOfNrmlMediaC5DescCodec2
,LstOfNrmlMediaC5DescBndW2
,LstOfNrmlMediaC5DescType3
,LstOfNrmlMediaC5DescCodec3
,LstOfNrmlMediaC5DescBndW3
,LstOfNrmlMediaC5medInitFlg
,LstOfNrmlMediaComponents6
,LstOfNrmlMediaComponents7
,LstOfNrmlMediaComponents8
,LstOfNrmlMediaComponents9
,LstOfNrmlMediaComponents10
,LstOfNrmlMediaComponents11
,LstOfNrmlMediaComponents12
,LstOfNrmlMediaComponents13
,LstOfNrmlMediaComponents14
,LstOfNrmlMediaComponents15
,LstOfNrmlMediaComponents16
,LstOfNrmlMediaComponents17
,LstOfNrmlMediaComponents18
,LstOfNrmlMediaComponents19
,LstOfNrmlMediaComponents20
,LstOfNrmlMediaComponents21
,LstOfNrmlMediaComponents22
,LstOfNrmlMediaComponents23
,LstOfNrmlMediaComponents24
,LstOfNrmlMediaComponents25
,LstOfNrmlMediaComponents26
,LstOfNrmlMediaComponents27
,LstOfNrmlMediaComponents28
,LstOfNrmlMediaComponents29
,LstOfNrmlMediaComponents30
,LstOfNrmlMediaComponents31
,LstOfNrmlMediaComponents32
,LstOfNrmlMediaComponents33
,LstOfNrmlMediaComponents34
,LstOfNrmlMediaComponents35
,LstOfNrmlMediaComponents36
,LstOfNrmlMediaComponents37
,LstOfNrmlMediaComponents38
,LstOfNrmlMediaComponents39
,LstOfNrmlMediaComponents40
,LstOfNrmlMediaComponents41
,LstOfNrmlMediaComponents42
,LstOfNrmlMediaComponents43
,LstOfNrmlMediaComponents44
,LstOfNrmlMediaComponents45
,LstOfNrmlMediaComponents46
,LstOfNrmlMediaComponents47
,LstOfNrmlMediaComponents48
,LstOfNrmlMediaComponents49
,LstOfNrmlMediaComponents50
,LstOfNrmlMedCompts1150
,GGSNAddress
,ServiceReasonReturnCode
,LstOfMessageBodies
,RecordExtension
,Expires
,Event
,Lst1AccessNetworkInfo
,Lst1AccessDomain
,Lst1AccessType
,Lst1LocationInfoType
,Lst1ChangeTime
,Lst1ChangeTimeNormalized
,Lst2AccessNetworkInfo
,Lst2AccessDomain
,Lst2AccessType
,Lst2LocationInfoType
,Lst2ChangeTime
,Lst2ChangeTimeNormalized
,Lst3AccessNetworkInfo
,Lst3AccessDomain
,Lst3AccessType
,Lst3LocationInfoType
,Lst3ChangeTime
,Lst3ChangeTimeNormalized
,Lst4AccessNetworkInfo
,Lst4AccessDomain
,Lst4AccessType
,Lst4LocationInfoType
,Lst4ChangeTime
,Lst4ChangeTimeNormalized
,Lst5AccessNetworkInfo
,Lst5AccessDomain
,Lst5AccessType
,Lst5LocationInfoType
,Lst5ChangeTime
,Lst5ChangeTimeNormalized
,Lst6AccessNetworkInfo
,Lst6AccessDomain
,Lst6AccessType
,Lst6LocationInfoType
,Lst6ChangeTime
,Lst6ChangeTimeNormalized
,Lst7AccessNetworkInfo
,Lst7AccessDomain
,Lst7AccessType
,Lst7LocationInfoType
,Lst7ChangeTime
,Lst7ChangeTimeNormalized
,Lst8AccessNetworkInfo
,Lst8AccessDomain
,Lst8AccessType
,Lst8LocationInfoType
,Lst8ChangeTime
,Lst8ChangeTimeNormalized
,Lst9AccessNetworkInfo
,Lst9AccessDomain
,Lst9AccessType
,Lst9LocationInfoType
,Lst9ChangeTime
,Lst9ChangeTimeNormalized
,Lst10AccessNetworkInfo
,Lst10AccessDomain
,Lst10AccessType
,Lst10LocationInfoType
,Lst10ChangeTime
,Lst10ChangeTimeNormalized
,ServiceContextID
--Phase 2 changes starts
,SubscriberE164_orig
--Phase 2 changes ends
,SubscriberE164
,SubscriberNo
,IMSI
,IMEI
,SubSIPURI
,NAI
,SubPrivate
,SubServedPartyDeviceID
--Phase 2 changes starts
,OtherPartyAddress_orig
--Phase 2 changes ends
,OtherPartyAddress
,PhoneType
,ServiceProvider
,HG_Group
--phase 2 changes starts
,End_User_SubsID_orig
,End_User_SubsID_CTN
,End_User_SubsID_Dom_fl
,End_User_SubsID_Dom_ty
--phase 2 changes ends
,LstOfEarlySDPMediaCmpnts
,IMSCommServiceIdentifier
,NumberPortabilityRouting
,CarrierSelectRouting
,SessionPriority
--phase 2 changes starts
,RequestedPartyAddress_orig
--phase 2 changes ends
,RequestedPartyAddress
,LstOfCalledAssertedID
,MMTelInfoAnalyzedCallType
,MTlInfoCalledAsrtIDPrsntSts
,MTlInfoCllgPrtyAddrPrsntSts
,MMTelInfoConferenceId
,MMTelInfoDialAroundIndctr
,MMTelInfoRelatedICID
,MMTelSplmtrySrvcInfo1ID
,MMTelSplmtrySrvcInfo1Act
--phase 2 changes starts
,MMTelSplmtrySrvcInfo1Redir_orig
--phase 2 changes ends
,MMTelSplmtrySrvcInfo1Redir
,MMTelSplmtrySrvcInfo2ID
,MMTelSplmtrySrvcInfo2Act
--phase 2 changes starts
,MMTelSplmtrySrvcInfo2Redir_orig
--phase 2 changes ends
,MMTelSplmtrySrvcInfo2Redir
,MMTelSplmtrySrvcInfo3ID
,MMTelSplmtrySrvcInfo3Act
--phase 2 changes starts
,MMTelSplmtrySrvcInfo3Redir_orig
--phase 2 changes ends
,MMTelSplmtrySrvcInfo3Redir
,MMTelSplmtrySrvcInfo4ID
,MMTelSplmtrySrvcInfo4Act
--phase 2 changes starts
,MMTelSplmtrySrvcInfo4Redir_orig
--phase 2 changes ends
,MMTelSplmtrySrvcInfo4Redir
,MMTelSplmtrySrvcInfo5ID
,MMTelSplmtrySrvcInfo5Act
--phase 2 changes starts
,MMTelSplmtrySrvcInfo5Redir_orig
--phase 2 changes ends
,MMTelSplmtrySrvcInfo5Redir
,MMTelInfoSplmtrySrvcInfo6
,MMTelInfoSplmtrySrvcInfo7
,MMTelInfoSplmtrySrvcInfo8
,MMTelInfoSplmtrySrvcInfo9
,MMTelInfoSplmtrySrvcInfo10
,MobileStationRoamingNumber
,TeleServiceCode
,TariffClass
,pVisitedNetworkID
--phase 2 changes starts
,networkCallType
,carrierIdCode
--phase 2 changes ends
---- Add control fields information
,file_name
,record_start
,record_end
,record_type
,family_name
,version_id
,file_time
,file_id
,switch_name
,num_records
,insert_timestamp
,SrvcRequestDttmNrml_Date
,CASE 
 WHEN Lst10AccessNetworkInfo IS not NULL THEN Lst10AccessNetworkInfo
 WHEN Lst9AccessNetworkInfo IS not NULL THEN Lst9AccessNetworkInfo
 WHEN Lst8AccessNetworkInfo IS not NULL THEN Lst8AccessNetworkInfo
 WHEN Lst7AccessNetworkInfo IS not NULL THEN Lst7AccessNetworkInfo
 WHEN Lst6AccessNetworkInfo IS not NULL THEN Lst6AccessNetworkInfo
 WHEN Lst5AccessNetworkInfo IS not NULL THEN Lst5AccessNetworkInfo
 WHEN Lst4AccessNetworkInfo IS not NULL THEN Lst4AccessNetworkInfo
 WHEN Lst3AccessNetworkInfo IS not NULL THEN Lst3AccessNetworkInfo
 WHEN Lst2AccessNetworkInfo IS not NULL THEN Lst2AccessNetworkInfo
 ELSE Null
END AS LAST_CELL
FROM FACT_UCC_CDR

) a
LEFT OUTER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) CGI_FIRST_CELL FROM cdrdm.DIM_CELL_SITE_INFO) b ON UPPER(CONCAT(SUBSTR(a.Lst1AccessNetworkInfo,1,3),SUBSTR(a.Lst1AccessNetworkInfo,5,3),SUBSTR(a.Lst1AccessNetworkInfo,9,4),REGEXP_EXTRACT(TRIM(SUBSTR(a.Lst1AccessNetworkInfo,14)), '(0*)(.*)', 2))) = UPPER(TRIM(b.CGI_HEX))
LEFT OUTER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) CGI_LAST_CELL FROM cdrdm.DIM_CELL_SITE_INFO) c ON UPPER(CONCAT(SUBSTR(a.LAST_CELL,1,3),SUBSTR(a.LAST_CELL,5,3),SUBSTR(a.LAST_CELL,9,4),REGEXP_EXTRACT(TRIM(SUBSTR(a.LAST_CELL,14)), '(0*)(.*)', 2))) = UPPER(TRIM(c.CGI_HEX))
WHERE ((b.CGI_FIRST_CELL = 1 AND c.CGI_LAST_CELL = 1) OR (b.CGI_FIRST_CELL IS NULL AND c.CGI_LAST_CELL = 1) OR (c.CGI_LAST_CELL IS NULL AND b.CGI_FIRST_CELL = 1) OR (b.CGI_FIRST_CELL IS NULL AND c.CGI_LAST_CELL IS NULL));