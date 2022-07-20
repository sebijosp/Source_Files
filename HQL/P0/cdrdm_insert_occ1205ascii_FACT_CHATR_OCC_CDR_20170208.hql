--[Version History]
--0.1 - danchang - 4/13/2016 - initial version
--0.2 - humane - 12/29/2016 - removed temp table  occ1205ascii_phys_device_temp and fixed duplicate bug in cdrdm.fact_chatr_occ_cdr table

SET hive.variable.substitute=true;
SET hive.execution.engine=mr;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=ORC;
SET hive.optimize.sort.dynamic.partition=false;

set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx8500m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx8500m;

SET WHERE_clause;

--Main Fact Table
INSERT INTO TABLE cdrdm.FACT_CHATR_OCC_CDR PARTITION (triggerDate)
SELECT
trim(resultCode),
trim(resultCodeExtension),
if((triggerTime is not NULL) AND LENGTH(triggerTime) > 13,from_unixtime(unix_timestamp(substr(triggerTime, 1, 14), 'yyyyMMddHHmmss')), NULL) AS triggerTime,
if((triggerTime is not NULL) AND LENGTH(triggerTime) > 0,if (INSTR(triggerTime, '+') > 0, SUBSTR(triggerTime,INSTR(triggerTime, '+')),if (INSTR(triggerTime, '-') > 0, SUBSTR(triggerTime,INSTR(triggerTime, '-')), NULL)),NULL) AS triggerTime_offset,
trim(nodeName),
trim(serviceContextID),
trim(chargingContextID),
trim(serviceSessionID),
trim(recordIdentificationNumber),
CAST(partialSequenceNumber AS BIGINT),
trim(lastPartialOutput),
if ((servedSubscriptionID is not NULL) AND LENGTH(SUBSTR(servedSubscriptionID, 1, INSTR(servedSubscriptionID, '|')-1)) > 0,
SUBSTR(servedSubscriptionID, 1, INSTR(servedSubscriptionID, '|')-1), NULL) AS Subscriber_no,
if(INSTR(servedSubscriptionID, '|') > 0, 
REGEXP_EXTRACT(SUBSTR(servedSubscriptionID, 1, INSTR(servedSubscriptionID, '|')-1), '(1*)(.*)', 2), NULL) AS TR_Subscriber_no,
if ((servedSubscriptionID is not NULL) AND LENGTH(SUBSTR(servedSubscriptionID, INSTR(servedSubscriptionID, '|')+1)) > 0, trim(SUBSTR(servedSubscriptionID, INSTR(servedSubscriptionID, '|')+1)), NULL) AS IMSI,
b.IMSI AS drvd_IMSI,
if (b.BAN is NULL, c.BAN, b.BAN) AS BAN,
--These fields are obtained from the join to cdrdm.mps_cust_1
c.account_sub_type AS ACCOUNT_SUB_TYPE,
c.company_code AS COMPANY_CODE,
c.sub_type AS SUB_TYPE,
c.tr_ba_company_code AS TR_BA_COMPANY_CODE,
c.tr_ba_company_name AS TR_BA_COMPANY_NAME,
c.tr_ba_account_type AS TR_BA_ACCOUNT_TYPE,
c.franchise_cd AS FRANCHISE_CD,
c.product_type AS product_type,
trim(correlationID_dccCorrelationId_acctMultiSessionID) AS crid_dcccrdid_multisess_id,
trim(correlationID_dccCorrelationId_ccCorrelationId) AS crid_dcccrdid_Correlaiton_id,
trim(correlationID_dccCorrelationId_sessionId) AS crid_dcccrdid_SessId,
trim(correlationID_dccCorrelationId_ccRequestNumber) AS crid_dcccrdid_ReqNo,
trim(servingElement_originInfo_originRealm) AS srvEle_originRealm,
trim(servingElement_originInfo_originHost) AS srvEle_originHost,
trim(creditControlRecords_serviceIdentifier) AS cc_serviceIdentifier,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[0]),NULL) AS tariff_Chg_Usg1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[1]),NULL) AS time_Unit1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[2]) AS BIGINT),NULL) AS money_Unit_amt1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[3]) AS BIGINT),NULL) AS money_Unit_decml1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[4]),NULL) AS money_Unit_currcy1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[5]) AS BIGINT),NULL) AS total_Octets_Unit1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[6]) AS BIGINT),NULL) AS uplink_Octets_Unit1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[7]) AS BIGINT),NULL) AS downlink_Octets_Unit1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[8]),NULL) AS service_Specific_Unit1,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[9]),NULL) AS tariff_Chg_Usg2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[10]),NULL) AS time_Unit2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[11]) AS BIGINT),NULL) AS money_Unit_amt2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[12]) AS BIGINT),NULL) AS money_Unit_decml2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[13]),NULL) AS money_Unit_currcy2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[14]) AS BIGINT),NULL) AS total_Octets_Unit2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[15]) AS BIGINT),NULL) AS uplink_Octets_Unit2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[16]) AS BIGINT),NULL) AS downlink_Octets_Unit2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[17]),NULL) AS service_Specific_Unit2,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[18]),NULL) AS tariff_Chg_Usg3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[19]),NULL) AS time_Unit3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[20]) AS BIGINT),NULL) AS money_Unit_amt3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[21]) AS BIGINT),NULL) AS money_Unit_decml3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[22]),NULL) AS money_Unit_currcy3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[23]) AS BIGINT),NULL)AS total_Octets_Unit3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[24]) AS BIGINT),NULL) AS uplink_Octets_Unit3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,CAST(trim(split(creditControlRecords_usedServiceUnits,'\\|')[25]) AS BIGINT),NULL) AS downlink_Octets_Unit3,
if(creditControlRecords_usedServiceUnits is not NULL and length(creditControlRecords_usedServiceUnits) > 0,trim(split(creditControlRecords_usedServiceUnits,'\\|')[26]),NULL) AS service_Specific_Unit3,
if((creditControlRecords_eventTime is not NULL) AND LENGTH(creditControlRecords_eventTime) > 13, from_unixtime(unix_timestamp(substr(creditControlRecords_eventTime, 1, 14), 'yyyyMMddHHmmss')),NULL) AS cc_eve_datetime,
if((creditControlRecords_eventTime is not NULL) AND LENGTH(creditControlRecords_eventTime) > 0,if (INSTR(creditControlRecords_eventTime, '+') > 0, SUBSTR(creditControlRecords_eventTime, INSTR(creditControlRecords_eventTime, '+')),if (INSTR(creditControlRecords_eventTime, '-') > 0, SUBSTR(creditControlRecords_eventTime, INSTR(creditControlRecords_eventTime, '-')),NULL)),NULL) AS cc_eve_time_offset,
if((creditControlRecords_triggerTime is not NULL) AND LENGTH(creditControlRecords_triggerTime) > 0,from_unixtime(unix_timestamp(substr(creditControlRecords_triggerTime, 1, 14), 'yyyyMMddHHmmss')),NULL) AS cc_trg_datetime,
if((creditControlRecords_triggerTime is not NULL) AND LENGTH(creditControlRecords_triggerTime) > 0,if (INSTR(creditControlRecords_triggerTime, '+') > 0, SUBSTR(creditControlRecords_triggerTime, INSTR(creditControlRecords_triggerTime, '+')),if (INSTR(creditControlRecords_triggerTime, '-') > 0, SUBSTR(creditControlRecords_triggerTime, INSTR(creditControlRecords_triggerTime, '-')),NULL)),NULL) AS cc_trg_time_offset,

trim(creditControlRecords_serviceScenario) AS cc_srv_scen,
trim(creditControlRecords_roamingPosition) AS cc_roaming_pos,
trim(creditControlRecords_tariffInfo) AS cc_tariff_info,
trim(creditControlRecords_cCAccountData) AS cc_cCAcct_dat,
trim(creditControlRecords_cCAccountData_accumulators) AS cc_cCAcctData_ac,
trim(ccr_ccad_da1_subdedicatedAccount) AS cc_subdedicated_acc1,
trim(ccr_ccad_da2_subdedicatedAccount) AS cc_subdedicated_acc2,
trim(ccr_ccad_da3_subdedicatedAccount) AS cc_subdedicated_acc3,
trim(ccr_ccad_da4_subdedicatedAccount) AS cc_subdedicated_acc4,
trim(ccr_ccad_da5_subdedicatedAccount) AS cc_subdedicated_acc5,
trim(creditControlRecords_cCAccountData2) AS cc_cCAcct_data,
trim(ccr_ccad_sc1_selectionTreeQualifiers) AS cc_sel_Tree_qua1,
trim(ccr_ccad_sc2_selectionTreeQualifiers) AS cc_sel_Tree_qua2,
trim(ccr_ccad_sc1_sca_units) AS cc_AcctD_sCon1_spCon_Acc,
trim(ccr_ccad_sc2_sca_units) AS cc_AcctD_sCon2_spCon_Acc,
trim(ccr_ccad_sc1_sca1_ucsc) AS cc_usgCntSpec_con1_acc1,
trim(ccr_ccad_sc1_sca2_ucsc) AS cc_usgCntSpec_con1_acc2,
trim(ccr_ccad_sc1_sca3_ucsc) AS cc_usgCntSpec_con1_acc3,
trim(ccr_ccad_sc1_sca4_ucsc) AS cc_usgCntSpec_con1_acc4,
trim(ccr_ccad_sc1_sca5_ucsc) AS cc_usgCntSpec_con1_acc5,
trim(ccr_ccad_sc1_sca6_ucsc) AS cc_usgCntSpec_con1_acc6,
trim(ccr_ccad_sc1_sca7_ucsc) AS cc_usgCntSpec_con1_acc7,
trim(ccr_ccad_sc2_sca1_ucsc) AS cc_usgCntSpec_con2_acc1,
trim(ccr_ccad_sc2_sca2_ucsc) AS cc_usgCntSpec_con2_acc2,
trim(ccr_ccad_sc2_sca3_ucsc) AS cc_usgCntSpec_con2_acc3,
trim(ccr_ccad_sc2_sca4_ucsc) AS cc_usgCntSpec_con2_acc4,
trim(ccr_ccad_sc2_sca5_ucsc) AS cc_usgCntSpec_con2_acc5,
trim(ccr_ccad_sc2_sca6_ucsc) AS cc_usgCntSpec_con2_acc6,
trim(ccr_ccad_sc2_sca7_ucsc) AS cc_usgCntSpec_con2_acc7,
trim(ccr_ccad_sc1_offerID) AS cc_cAcctData_spec_Cons1_off,
trim(ccr_ccad_sc2_offerID) AS cc_cAcctData_spec_Cons2_off,
trim(ccr_ccad_sc1_terminationAtUnit) AS cc_term_cos1_At_uni,
trim(ccr_ccad_sc2_terminationAtUnit) AS cc_term_cos2_At_uni,
trim(ccr_ccad_sc1_eligibleOfferAtUnit) AS cc_cos1_At_egl_offr,
trim(ccr_ccad_sc2_eligibleOfferAtUnit) AS cc_cos2_At_egl_offr,
trim(ccr_ccad_sc1_offerProviderID) AS cc_cos1_offr_pro,
trim(ccr_ccad_sc2_offerProviderID) AS cc_cos2_offr_pro,
trim(ccr_ccad_sc1_productID) AS cc_cos1_prodid,
trim(ccr_ccad_sc2_productID) AS cc_cos2_prodid,
trim(ccr_ccad_sc1_offerAttributes) AS cc_cos1_At_offr_attr,
trim(ccr_ccad_sc2_offerAttributes) AS cc_cos2_At_offr_attr,
trim(creditControlRecords_cCAccountData_usedOffers) AS cc_used_offer,
trim(creditControlRecords_cCAccountData_usageCounters) AS cc_usage_cou,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778238') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778238') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778238' )),'\\|')[1])), NULL) AS Gy_Charg_Charact,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778226') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778226') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778226' )),'\\|')[1])), NULL) AS Gy_Charging_Id,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778233') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778233') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778233' )),'\\|')[1])), NULL) AS Gy_GGSN_MCC_MNC,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778232') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778232') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778232' )),'\\|')[1])), NULL) AS Gy_IMSI_MCC_MNC,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778234') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778234') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778234' )),'\\|')[1])), NULL) AS Gy_NSAPI,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778227') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778227') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778227' )),'\\|')[1])), NULL) AS Gy_PDP_Type,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778240') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778240') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778240' )),'\\|')[1])), NULL) AS Gy_RAT_Type,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778239') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778239') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778239' )),'\\|')[1])), NULL) AS Gy_SGSN_MCC_MNC,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778237') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778237') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778237' )),'\\|')[1])), NULL) AS Gy_Selection_Mode,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778236') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778236') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778236' )),'\\|')[1])), NULL) AS Gy_Sess_Stop_Ind,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778231') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778231') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778231' )),'\\|')[1])), NULL) AS Gy_CG_Address,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778235') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778235') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778235' )),'\\|')[1])), NULL) AS Gy_Called_Stat_Id,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778251') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778251') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778251' )),'\\|')[1])), NULL) AS Gy_Cell_Identity,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778332') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778332') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778332' )),'\\|')[1])), NULL) AS Gy_ECID,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778253') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778253') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778253' )),'\\|')[1])), NULL) AS Gy_Routing_Area_Code,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778252') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778252') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778252' )),'\\|')[1])), NULL) AS Gy_Service_Area_Code,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778333') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778333') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778333' )),'\\|')[1])), NULL) AS Gy_Tracking_Area_Code,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778250') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778250') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778250' )),'\\|')[1])), NULL) AS Gy_Location_Area_Code,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778249') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778249') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778249' )),'\\|')[1])), NULL) AS Gy_Location_MCC_MNC,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778230') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778230') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778230' )),'\\|')[1])), NULL) AS Gy_GGSN_Address,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778242') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778242') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778242' )),'\\|')[1])), NULL) AS Gy_NAS_Port,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778228') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778228') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778228' )),'\\|')[1])), NULL) AS Gy_PDP_Address,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778241') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778241') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778241' )),'\\|')[1])), NULL) AS Gy_PDP_Context_Type,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778254') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778254') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778254' )),'\\|')[1])), NULL) AS Gy_Rating_Group,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778229') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778229') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778229' )),'\\|')[1])), NULL) AS Gy_SGSN_Address,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778243') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778243') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778243' )),'\\|')[1])), NULL) AS Gy_IMEISV,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778265') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778265') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778265' )),'\\|')[1])), NULL) AS Gy_QoS_Class_Iden,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778258') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778258') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778258' )),'\\|')[1])), NULL) AS Gy_Max_Req_Band_UL,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778257') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778257') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778257' )),'\\|')[1])), NULL) AS Gy_Max_Req_Band_DL,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778406') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778406') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778406' )),'\\|')[1])), NULL) AS Gy_Guaran_Bitrate_UL,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778407') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778407') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778407' )),'\\|')[1])), NULL) AS Gy_Guaran_Bitrate_DL,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778408') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778408') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778408' )),'\\|')[1])), NULL) AS Gy_Bearer_Identifier,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778259') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778259') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778259' )),'\\|')[1])), NULL) AS Gy_Priority_Level,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778260') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778260') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778260' )),'\\|')[1])), NULL) AS Gy_Pre_emption_Capability,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778261') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778261') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778261' )),'\\|')[1])), NULL) AS Gy_Pre_emption_Vulnerability,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778409') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778409') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778409' )),'\\|')[1])), NULL) AS Gy_APN_Agg_Max_Bitrate_UL,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778410') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778410') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778410' )),'\\|')[1])), NULL) AS Gy_APN_Agg_Max_Bitrate_DL,
if(INSTR(creditControlRecords_chargingContextSpecific, '16778411') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778411') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778411' )),'\\|')[1])), NULL) AS Gy_Node_Id,
CONCAT('302-720-', CONV(
--start of Gy_Location_Area_Code transformation logic
if(INSTR(creditControlRecords_chargingContextSpecific, '16778250') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778250') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778250' )),'\\|')[1])), NULL)
--end of Gy_Location_Area_Code transform logic
,16,10),'-',CONV( if(
-- Convert (Gy_Cell_Identity or Gy_Service_Area_Code)
--start of Gy_Cell_Identity
if(INSTR(creditControlRecords_chargingContextSpecific, '16778251') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778251') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778251' )),'\\|')[1])), NULL)
--end of Gy_Cell_Identity
is NULL,
--start of Gy_Service_Area_Code
if(INSTR(creditControlRecords_chargingContextSpecific, '16778252') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778252') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778252' )),'\\|')[1])), NULL)
--end of Gy_Service_Area_Code
,
--start of Gy_Cell_Identity
if(INSTR(creditControlRecords_chargingContextSpecific, '16778251') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778251') is NOT NULL,
trim(UPPER(SUBSTR(creditControlRecords_chargingContextSpecific, INSTR(creditControlRecords_chargingContextSpecific, '16778251') + 9,
(locate('|', creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778251') + 9)
 - (INSTR(creditControlRecords_chargingContextSpecific, '16778251') + 9))))), NULL)
--end of Gy_Cell_Identity
),16,10)
) AS Cell_ID_DEC,
CONCAT('302720',
--start of Gy_Location_Area_Code transformation logic
if(INSTR(creditControlRecords_chargingContextSpecific, '16778250') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778250') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778250' )),'\\|')[1])), NULL)
--end of Gy_Location_Area_Code transform logic
,
-- Convert (Gy_Cell_Identity or Gy_Service_Area_Code)
if(
--start of Gy_Cell_Identity
if(INSTR(creditControlRecords_chargingContextSpecific, '16778251') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778251') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778251' )),'\\|')[1])), NULL)
--end of Gy_Cell_Identity
is NULL,
--start of Gy_Service_Area_Code
if(INSTR(creditControlRecords_chargingContextSpecific, '16778252') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778252') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778252' )),'\\|')[1])), NULL)
--end of Gy_Service_Area_Code
,
--start of Gy_Cell_Identity
if(INSTR(creditControlRecords_chargingContextSpecific, '16778251') > 0 AND INSTR(creditControlRecords_chargingContextSpecific, '16778251') is NOT NULL,trim(UPPER(split(substr(creditControlRecords_chargingContextSpecific,INSTR(creditControlRecords_chargingContextSpecific, '16778251' )),'\\|')[1])), NULL)
--end of Gy_Cell_Identity
)
) AS Cell_ID_Hex,

trim(creditControlRecords_treeDefinedFields1) AS cc_treeDefined_fld1,
trim(creditControlRecords_treeDefinedFields2) AS cc_treeDefined_fld2,
trim(creditControlRecords_treeDefinedFields3) AS cc_treeDefined_fld3,
trim(ccr_ba1) AS cc_bonus_adj,
trim(ccr_ba1_accumulators) AS cc_bonusAdj_accum,
trim(ccr_ba1_dedicatedAccounts1_subDedicatedAccounts) AS cc_subDed_acct1,
trim(ccr_ba1_dedicatedAccounts2_subDedicatedAccounts) AS cc_subDed_acct2,
trim(ccr_ba1_dedicatedAccounts3_subDedicatedAccounts) AS cc_subDed_acct3,
trim(ccr_ba1_dedicatedAccounts4_subDedicatedAccounts) AS cc_subDed_acct4,
trim(ccr_ba1_dedicatedAccounts5_subDedicatedAccounts) AS cc_subDed_acct5,
trim(ccr_ba_lifeCycleInformation) AS cc_Bon_adj_lifeCyc_inf,
trim(ccr_ba_bonusOffers) AS cc_bonus_Adj_offer,
trim(creditControlRecords_serviceSetupResult) AS ccl_serv_setup_result,
trim(creditControlRecords_terminationCause) AS ccl_serv_term_cause,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[0]) AS BIGINT),NULL) AS cc_dbt_rtd_TimeUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[1]) AS BIGINT),NULL) AS cc_dbt_rtd_totalOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[2]) AS BIGINT),NULL) AS cc_dbt_rtd_UpOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[3]) AS BIGINT),NULL) AS cc_dbt_rtd_DownOcsUnit,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[4]) AS BIGINT),NULL) AS cc_dbt_rtd_ServSpecUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[5]) AS BIGINT),NULL) AS cc_crd_rtd_TimeUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[6]) AS BIGINT),NULL) AS cc_crd_rtd_TotalOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[7]) AS BIGINT),NULL) AS cc_crd_rtd_UpOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[8]) AS BIGINT),NULL) AS cc_crd_rtd_DownOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[9]) AS BIGINT),NULL) AS cc_crd_rtd_ServSpecUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[10]) AS BIGINT),NULL) AS cc_free_rtd_TimeUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[11]) AS BIGINT),NULL) AS cc_free_rtd_TotalOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[12]) AS BIGINT),NULL) AS cc_free_rtd_UpOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[13]) AS BIGINT),NULL) AS cc_free_rtd_DownOctUnits,
if(creditControlRecords_ratedUnits is not NULL and length(creditControlRecords_ratedUnits) > 0,CAST(trim(split(creditControlRecords_ratedUnits,'\\|')[14]) AS BIGINT),NULL) AS cc_free_rtd_ServSpecUnits,
trim(creditControlRecords_periodicAccountMgmtData) AS cc_prdic_Acct_Mgmt_data,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,trim(split(usedUnchargedServiceUnits,'\\|')[0]),NULL) AS UnchrgdSU_tariffChngUsg1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[1]) AS BIGINT),NULL) AS UnchrgdSU_timeUnit1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[2]) AS BIGINT),NULL) AS UnchrgdSU_UnitAmt1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[3]) AS BIGINT),NULL) AS UnchrgdSU_Unit_dcml1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[4]) AS BIGINT),NULL) AS UnchrgdSU_Unit_currcy1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[5]) AS BIGINT),NULL) AS UnchrgdSU_totalOct1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[6]) AS BIGINT),NULL) AS UnchrgdSU_upOct1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[7]) AS BIGINT),NULL) AS UnchrgdSU_downOct1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[8]) AS BIGINT),NULL) AS UnchrgdSU_srvSp1,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,trim(split(usedUnchargedServiceUnits,'\\|')[9]),NULL) AS UnchrgdSU_tariffChngUsg2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[10]) AS BIGINT),NULL) AS UnchrgdSU_timeUnit2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[11]) AS BIGINT),NULL) AS UnchrgdSU_UnitAmt2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[12]) AS BIGINT),NULL) AS UnchrgdSU_Unit_dcml2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[13]) AS BIGINT),NULL) AS UnchrgdSU_Unit_currcy2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[14]) AS BIGINT),NULL) AS UnchrgdSU_totalOct2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[15]) AS BIGINT),NULL) AS UnchrgdSU_upOct2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[16]) AS BIGINT),NULL) AS UnchrgdSU_downOct2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[17]) AS BIGINT),NULL) AS UnchrgdSU_srvSp2,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,trim(split(usedUnchargedServiceUnits,'\\|')[18]),NULL) AS UnchrgdSU_tariffChngUsg3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[19]) AS BIGINT),NULL) AS UnchrgdSU_timeUnit3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[20]) AS BIGINT),NULL) AS UnchrgdSU_UnitAmt3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[21]) AS BIGINT),NULL) AS UnchrgdSU_Unit_dcml3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[22]) AS BIGINT),NULL) AS UnchrgdSU_Unit_currcy3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[23]) AS BIGINT),NULL) AS UnchrgdSU_totalOct3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[24]) AS BIGINT),NULL) AS UnchrgdSU_upOct3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[25]) AS BIGINT),NULL) AS UnchrgdSU_downOct3,
if(usedUnchargedServiceUnits is not NULL and length(usedUnchargedServiceUnits) > 0,CAST(trim(split(usedUnchargedServiceUnits,'\\|')[26]) AS BIGINT),NULL) AS UnchrgdSU_srvSp3,
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()),
CONCAT(SUBSTR(triggerTime,1,4),'-',SUBSTR(triggerTime,5,2),'-',SUBSTR(triggerTime,7,2)) AS triggerDate
FROM cdrdm.occ1205ascii a
LEFT OUTER JOIN
(Select z.imsi AS IMSI,z.ban AS BAN,z.subscriber_no AS SUBSCRIBER_NO,z.effective_date AS EFFECTIVE_DATE,z.ESN_SEQ_NO from 
(
Select y.imsi AS IMSI,y.ban AS BAN,y.subscriber_no AS SUBSCRIBER_NO,y.effective_date AS EFFECTIVE_DATE,
y.ESN_SEQ_NO  as ESN_SEQ_NO, row_number() over (partition by y.subscriber_no order by y.ESN_SEQ_NO desc) as r from 
(
select * from (Select * from cdrdm.occ1205ascii a ${hiveconf:WHERE_clause}) w Right outer join
(select * from ela_v21.physical_device where eq_type='G')x
on REGEXP_EXTRACT(SUBSTR(w.servedSubscriptionID, 1, INSTR(w.servedSubscriptionID, '|')-1), '(1*)(.*)', 2) = x.Subscriber_no
where x.effective_date < CONCAT(SUBSTR(w.triggerTime,1,4),'-',SUBSTR(w.triggerTime,5,2),'-',SUBSTR(w.triggerTime,7,2),' ',
SUBSTR(w.triggerTime,9,2),':',SUBSTR(w.triggerTime,11,2),':',SUBSTR(w.triggerTime,13,2))
)y
)z where z.r =1
) b
--Join with dim_mps_cust_1 to obtain account and other fields
LEFT OUTER JOIN (SELECT * FROM cdrdm.dim_mps_cust_1 WHERE mps_cust_seq_no = 1 AND product_type = 'C') c
ON REGEXP_EXTRACT(SUBSTR(a.servedSubscriptionID, 1, INSTR(a.servedSubscriptionID, '|')-1), '(1*)(.*)', 2) = c.Subscriber_no

${hiveconf:WHERE_clause};

