--[Version History]
--0.1 - danchang - 3/17/2016 - initial creation
--0.2 - danchang - 3/21/2016 - rewritten after new logic description
--0.3 - danchang - 4/06/2016 - updated syntax and test table locations
--e.g. verticalcoordinate -> vertical_coordinate, ela_v21.local_calling_area_gg -> ext_ela_v21.local_calling_area_gg
--0.4 - danchang - 4/29/2016 - updated ext_ela_v21 -> ela_v21
--0.5 - saseenthar - 6/13/2016 - included new fields from UCC/IMM cdrs as part of UCC Phase 2 Requirement
--modified to include missing selection criteria for UCC cdrs as part of UCC Phase 2 Requirement
--modified join prediction route_id derivtion logic in ext_cdrdm.v_MO_LD_UCC_temp2 data load as part of UCC Phase 2 Requirement

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

USE ext_cdrdm;

DROP VIEW IF EXISTS v_MO_LD_UCC_NPA_NXX_STATE_MPS;
DROP VIEW IF EXISTS v_MO_LD_UCC_LOCAL_CALLING_AREA;
DROP VIEW IF EXISTS v_MO_LD_UCC_temp1;
DROP VIEW IF EXISTS v_MO_LD_UCC_temp2;
DROP VIEW IF EXISTS v_MO_LD_UCC_temp3;
DROP VIEW IF EXISTS v_MO_LD_UCC_temp4;

CREATE VIEW IF NOT EXISTS v_MO_LD_UCC_NPA_NXX_STATE_MPS AS
SELECT 
a.Npa, a.Nxx, a.Place_Name, a.State_Code, a.V_Coordinates, a.H_Coordinates, b.STATE_NAME, b.COUNTRY_CODE
FROM
ela_v21.npa_nxx_gg a
LEFT OUTER JOIN cdrdm.dim_state_mps b
ON a.state_code = b.state_code
WHERE discontinue_date > from_unixtime(unix_timestamp());

CREATE VIEW IF NOT EXISTS v_MO_LD_UCC_LOCAL_CALLING_AREA AS
SELECT
DISTINCT sid, Vertical_Coordinate, Horizontal_Cordinate, 'FLCA' AS LCA_FLAG
FROM ela_v21.local_calling_area_gg
WHERE discontinue_date > from_unixtime(unix_timestamp());

CREATE VIEW IF NOT EXISTS v_MO_LD_UCC_temp1 AS 
SELECT
recordtype,
retransmission,
sipmethod,
roleofnode,
nodeaddress,
sessionid,
callingpartyaddress_orig,
callingpartyaddress,
calledpartyaddress_orig,
calledpartyaddress,
srvcrequesttimestamp,
srvcdeliverystarttimestamp,
srvcdeliveryendtimestamp,
recordopeningtime,
recordclosuretime,
srvcrequestdttmnrml,
srvcdeliverystartdttmnrml,
srvcdeliveryenddttmnrml,
chargeableduration,
tmfromsiprqststartofchrgng,
interoperatoridentifiers,
localrecordsequencenumber,
partialoutputrecordnumber,
causeforrecordclosing_orig,
causecodepreestablishedsession,
reasoncodepreestablishedsession,
causecodeestablishedsession,
reasoncodeestablishedsession,
incomplete_cdr_ind_orig,
acrstartlost,
acrinterimlost,
acrstoplost,
imsilost,
acrsccasstartlost,
acrsccasinterimlost,
acrsccasstoplost,
imschargingidentifier,
lstofsdpmediacomponents,
lstofnrmlmediac1chgtime,
lstofnrmlmediac1chgtimenor,
lstofnrmlmediac1desctype1,
lstofnrmlmediac1desccodec1,
lstofnrmlmediac1descbndw1,
lstofnrmlmediac1desctype2,
lstofnrmlmediac1desccodec2,
lstofnrmlmediac1descbndw2,
lstofnrmlmediac1desctype3,
lstofnrmlmediac1desccodec3,
lstofnrmlmediac1descbndw3,
lstofnrmlmediac1medinitflg,
lstofnrmlmediac2chgtime,
lstofnrmlmediac2chgtimenor,
lstofnrmlmediac2desctype1,
lstofnrmlmediac2desccodec1,
lstofnrmlmediac2descbndw1,
lstofnrmlmediac2desctype2,
lstofnrmlmediac2desccodec2,
lstofnrmlmediac2descbndw2,
lstofnrmlmediac2desctype3,
lstofnrmlmediac2desccodec3,
lstofnrmlmediac2descbndw3,
lstofnrmlmediac2medinitflg,
lstofnrmlmediac3chgtime,
lstofnrmlmediac3chgtimenor,
lstofnrmlmediac3desctype1,
lstofnrmlmediac3desccodec1,
lstofnrmlmediac3descbndw1,
lstofnrmlmediac3desctype2,
lstofnrmlmediac3desccodec2,
lstofnrmlmediac3descbndw2,
lstofnrmlmediac3desctype3,
lstofnrmlmediac3desccodec3,
lstofnrmlmediac3descbndw3,
lstofnrmlmediac3medinitflg,
lstofnrmlmediac4chgtime,
lstofnrmlmediac4chgtimenor,
lstofnrmlmediac4desctype1,
lstofnrmlmediac4desccodec1,
lstofnrmlmediac4descbndw1,
lstofnrmlmediac4desctype2,
lstofnrmlmediac4desccodec2,
lstofnrmlmediac4descbndw2,
lstofnrmlmediac4desctype3,
lstofnrmlmediac4desccodec3,
lstofnrmlmediac4descbndw3,
lstofnrmlmediac4medinitflg,
lstofnrmlmediac5chgtime,
lstofnrmlmediac5chgtimenor,
lstofnrmlmediac5desctype1,
lstofnrmlmediac5desccodec1,
lstofnrmlmediac5descbndw1,
lstofnrmlmediac5desctype2,
lstofnrmlmediac5desccodec2,
lstofnrmlmediac5descbndw2,
lstofnrmlmediac5desctype3,
lstofnrmlmediac5desccodec3,
lstofnrmlmediac5descbndw3,
lstofnrmlmediac5medinitflg,
lstofnrmlmediacomponents6,
lstofnrmlmediacomponents7,
lstofnrmlmediacomponents8,
lstofnrmlmediacomponents9,
lstofnrmlmediacomponents10,
lstofnrmlmediacomponents11,
lstofnrmlmediacomponents12,
lstofnrmlmediacomponents13,
lstofnrmlmediacomponents14,
lstofnrmlmediacomponents15,
lstofnrmlmediacomponents16,
lstofnrmlmediacomponents17,
lstofnrmlmediacomponents18,
lstofnrmlmediacomponents19,
lstofnrmlmediacomponents20,
lstofnrmlmediacomponents21,
lstofnrmlmediacomponents22,
lstofnrmlmediacomponents23,
lstofnrmlmediacomponents24,
lstofnrmlmediacomponents25,
lstofnrmlmediacomponents26,
lstofnrmlmediacomponents27,
lstofnrmlmediacomponents28,
lstofnrmlmediacomponents29,
lstofnrmlmediacomponents30,
lstofnrmlmediacomponents31,
lstofnrmlmediacomponents32,
lstofnrmlmediacomponents33,
lstofnrmlmediacomponents34,
lstofnrmlmediacomponents35,
lstofnrmlmediacomponents36,
lstofnrmlmediacomponents37,
lstofnrmlmediacomponents38,
lstofnrmlmediacomponents39,
lstofnrmlmediacomponents40,
lstofnrmlmediacomponents41,
lstofnrmlmediacomponents42,
lstofnrmlmediacomponents43,
lstofnrmlmediacomponents44,
lstofnrmlmediacomponents45,
lstofnrmlmediacomponents46,
lstofnrmlmediacomponents47,
lstofnrmlmediacomponents48,
lstofnrmlmediacomponents49,
lstofnrmlmediacomponents50,
lstofnrmlmedcompts1150,
ggsnaddress,
servicereasonreturncode,
lstofmessagebodies,
recordextension,
expires,
event,
lst1accessnetworkinfo AS Lst1AccessNetworkInfo,
lst1accessdomain,
lst1accesstype,
lst1locationinfotype,
lst1changetime,
lst1changetimenormalized,
lst2accessnetworkinfo,
lst2accessdomain,
lst2accesstype,
lst2locationinfotype,
lst2changetime,
lst2changetimenormalized,
lst3accessnetworkinfo,
lst3accessdomain,
lst3accesstype,
lst3locationinfotype,
lst3changetime,
lst3changetimenormalized,
lst4accessnetworkinfo,
lst4accessdomain,
lst4accesstype,
lst4locationinfotype,
lst4changetime,
lst4changetimenormalized,
lst5accessnetworkinfo,
lst5accessdomain,
lst5accesstype,
lst5locationinfotype,
lst5changetime,
lst5changetimenormalized,
lst6accessnetworkinfo,
lst6accessdomain,
lst6accesstype,
lst6locationinfotype,
lst6changetime,
lst6changetimenormalized,
lst7accessnetworkinfo,
lst7accessdomain,
lst7accesstype,
lst7locationinfotype,
lst7changetime,
lst7changetimenormalized,
lst8accessnetworkinfo,
lst8accessdomain,
lst8accesstype,
lst8locationinfotype,
lst8changetime,
lst8changetimenormalized,
lst9accessnetworkinfo,
lst9accessdomain,
lst9accesstype,
lst9locationinfotype,
lst9changetime,
lst9changetimenormalized,
lst10accessnetworkinfo,
lst10accessdomain,
lst10accesstype,
lst10locationinfotype,
lst10changetime,
lst10changetimenormalized,
servicecontextid,
--Phase 2 changes starts
SubscriberE164_orig,
--Phase 2 changes ends	
subscribere164,
subscriberno,
imsi,
imei,
subsipuri,
nai,
subprivate,
subservedpartydeviceid,
--Phase 2 changes starts
OtherPartyAddress_orig,
--Phase 2 changes ends	
otherpartyaddress,
phonetype,
serviceprovider,
hg_group,
--Phase 2 changes starts
End_User_SubsID_orig,
End_User_SubsID_CTN,
End_User_SubsID_Dom_fl,
End_User_SubsID_Dom_ty,
--Phase 2 changes ends	
lstofearlysdpmediacmpnts,
imscommserviceidentifier,
numberportabilityrouting,
carrierselectrouting,
sessionpriority,
--Phase 2 changes starts
RequestedPartyAddress_orig,
--Phase 2 changes ends
requestedpartyaddress,
lstofcalledassertedid,
mmtelinfoanalyzedcalltype,
mtlinfocalledasrtidprsntsts,
mtlinfocllgprtyaddrprsntsts,
mmtelinfoconferenceid,
mmtelinfodialaroundindctr,
mmtelinforelatedicid,
mmtelsplmtrysrvcinfo1id,
mmtelsplmtrysrvcinfo1act,
--Phase 2 changes starts
MMTelSplmtrySrvcInfo1Redir_orig,
--Phase 2 changes ends	
mmtelsplmtrysrvcinfo1redir,
mmtelsplmtrysrvcinfo2id,
mmtelsplmtrysrvcinfo2act,
--Phase 2 changes starts
MMTelSplmtrySrvcInfo2Redir_orig,
--Phase 2 changes ends	
mmtelsplmtrysrvcinfo2redir,
NULL AS MMTelInfoSplmtrySrvcInfo3,
mmtelsplmtrysrvcinfo3id,
mmtelsplmtrysrvcinfo3act,
--Phase 2 changes starts
MMTelSplmtrySrvcInfo3Redir_orig,
--Phase 2 changes ends	
mmtelsplmtrysrvcinfo3redir,
NULL AS MMTelInfoSplmtrySrvcInfo4,
mmtelsplmtrysrvcinfo4id,
mmtelsplmtrysrvcinfo4act,
--Phase 2 changes starts
MMTelSplmtrySrvcInfo4Redir_orig,
--Phase 2 changes ends	
mmtelsplmtrysrvcinfo4redir,
NULL AS MMTelInfoSplmtrySrvcInfo5,
mmtelsplmtrysrvcinfo5id,
mmtelsplmtrysrvcinfo5act,
--Phase 2 changes starts
MMTelSplmtrySrvcInfo5Redir_orig,
--Phase 2 changes ends
mmtelsplmtrysrvcinfo5redir,
mmtelinfosplmtrysrvcinfo6,
mmtelinfosplmtrysrvcinfo7,
mmtelinfosplmtrysrvcinfo8,
mmtelinfosplmtrysrvcinfo9,
mmtelinfosplmtrysrvcinfo10,
mobilestationroamingnumber,
teleservicecode,
tariffclass,
pvisitednetworkid,
--Phase 2 changes starts
networkCallType,
carrierIdCode,
--Phase 2 changes ends	
tr_ban,
tr_account_sub_type,
tr_company_code,
tr_franchise_cd,
--Phase 2 changes starts
tr_product_type,
--Phase 2 changes ends
tr_ba_account_type,
tr_sub_als_ind,
tr_sub_oth_party_als_ind,
tr_re_dir1_als_ind,
tr_sub_ind_gen,
--Phase 2 changes starts
WL_BAN,
WL_ACCOUNT_SUB_TYPE,
WL_COMPANY_CODE,
WL_FRANCHISE_CD,
WL_PRODUCT_TYPE,
WL_BA_ACCOUNT_TYPE,      
-----------
WL_SUB_ALS_IND,
WL_SUB_OTH_PARTY_ALS_IND,
WL_RE_DIR1_ALS_IND,
--Phase 2 changes ends
'UCC' AS CDR_type
FROM cdrdm.fact_ucc_cdr
--Phase 2 changes starts - modified logic
WHERE SIPMETHOD ='INVITE' 
AND RoleOfNode = 0 AND MMTelSplmtrySrvcInfo1ID NOT IN ('901','902','903','904','905','906','907','908','909','910','911',
'912','913','0','1','2','3','4','5','10','11','12','13','14','15','20','30','31')
--Phase 2 changes ends

UNION ALL

SELECT
recordtype,
retransmission,
sipmethod,
roleofnode,
nodeaddress,
sessionid,
NULL AS CallingPartyAddress_orig,
callingpartyaddress,
NULL AS CalledPartyAddress_orig,
calledpartyaddress,
srvcrequesttimestamp,
srvcdeliverystarttimestamp,
srvcdeliveryendtimestamp,
recordopeningtime,
recordclosuretime,
srvcrequestdttmnrml,
srvcdeliverystartdttmnrml,
srvcdeliveryenddttmnrml,
chargeableduration,
tmfromsiprqststartofchrgng,
interoperatoridentifiers,
localrecordsequencenumber,
partialoutputrecordnumber,
causeforrecordclosing AS CauseForRecordClosing_Orig,
NULL AS causeCodePreEstablishedSession,
NULL AS reasonCodePreEstablishedSession,
NULL AS causeCodeEstablishedSession,
NULL AS reasonCodeEstablishedSession,
NULL AS incomplete_CDR_Ind_orig,
acrstartlost,
acrinterimlost,
acrstoplost,
imsilost,
acrsccasstartlost,
acrsccasinterimlost,
acrsccasstoplost,
imschargingidentifier,
lstofsdpmediacomponents,
lstofnrmlmediac1chgtime,
lstofnrmlmediac1chgtimenor,
lstofnrmlmediac1desctype1,
lstofnrmlmediac1desccodec1,
lstofnrmlmediac1descbndw1,
lstofnrmlmediac1desctype2,
lstofnrmlmediac1desccodec2,
lstofnrmlmediac1descbndw2,
lstofnrmlmediac1desctype3,
lstofnrmlmediac1desccodec3,
lstofnrmlmediac1descbndw3,
lstofnrmlmediac1medinitflg,
lstofnrmlmediac2chgtime,
lstofnrmlmediac2chgtimenor,
lstofnrmlmediac2desctype1,
lstofnrmlmediac2desccodec1,
lstofnrmlmediac2descbndw1,
lstofnrmlmediac2desctype2,
lstofnrmlmediac2desccodec2,
lstofnrmlmediac2descbndw2,
lstofnrmlmediac2desctype3,
lstofnrmlmediac2desccodec3,
lstofnrmlmediac2descbndw3,
lstofnrmlmediac2medinitflg,
lstofnrmlmediac3chgtime,
lstofnrmlmediac3chgtimenor,
lstofnrmlmediac3desctype1,
lstofnrmlmediac3desccodec1,
lstofnrmlmediac3descbndw1,
lstofnrmlmediac3desctype2,
lstofnrmlmediac3desccodec2,
lstofnrmlmediac3descbndw2,
lstofnrmlmediac3desctype3,
lstofnrmlmediac3desccodec3,
lstofnrmlmediac3descbndw3,
lstofnrmlmediac3medinitflg,
lstofnrmlmediac4chgtime,
lstofnrmlmediac4chgtimenor,
lstofnrmlmediac4desctype1,
lstofnrmlmediac4desccodec1,
lstofnrmlmediac4descbndw1,
lstofnrmlmediac4desctype2,
lstofnrmlmediac4desccodec2,
lstofnrmlmediac4descbndw2,
lstofnrmlmediac4desctype3,
lstofnrmlmediac4desccodec3,
lstofnrmlmediac4descbndw3,
lstofnrmlmediac4medinitflg,
lstofnrmlmediac5chgtime,
lstofnrmlmediac5chgtimenor,
lstofnrmlmediac5desctype1,
lstofnrmlmediac5desccodec1,
lstofnrmlmediac5descbndw1,
lstofnrmlmediac5desctype2,
lstofnrmlmediac5desccodec2,
lstofnrmlmediac5descbndw2,
lstofnrmlmediac5desctype3,
lstofnrmlmediac5desccodec3,
lstofnrmlmediac5descbndw3,
lstofnrmlmediac5medinitflg,
lstofnrmlmediacomponents6,
lstofnrmlmediacomponents7,
lstofnrmlmediacomponents8,
lstofnrmlmediacomponents9,
lstofnrmlmediacomponents10,
lstofnrmlmediacomponents11,
lstofnrmlmediacomponents12,
lstofnrmlmediacomponents13,
lstofnrmlmediacomponents14,
lstofnrmlmediacomponents15,
lstofnrmlmediacomponents16,
lstofnrmlmediacomponents17,
lstofnrmlmediacomponents18,
lstofnrmlmediacomponents19,
lstofnrmlmediacomponents20,
lstofnrmlmediacomponents21,
lstofnrmlmediacomponents22,
lstofnrmlmediacomponents23,
lstofnrmlmediacomponents24,
lstofnrmlmediacomponents25,
lstofnrmlmediacomponents26,
lstofnrmlmediacomponents27,
lstofnrmlmediacomponents28,
lstofnrmlmediacomponents29,
lstofnrmlmediacomponents30,
lstofnrmlmediacomponents31,
lstofnrmlmediacomponents32,
lstofnrmlmediacomponents33,
lstofnrmlmediacomponents34,
lstofnrmlmediacomponents35,
lstofnrmlmediacomponents36,
lstofnrmlmediacomponents37,
lstofnrmlmediacomponents38,
lstofnrmlmediacomponents39,
lstofnrmlmediacomponents40,
lstofnrmlmediacomponents41,
lstofnrmlmediacomponents42,
lstofnrmlmediacomponents43,
lstofnrmlmediacomponents44,
lstofnrmlmediacomponents45,
lstofnrmlmediacomponents46,
lstofnrmlmediacomponents47,
lstofnrmlmediacomponents48,
lstofnrmlmediacomponents49,
lstofnrmlmediacomponents50,
lstofnrmlmedcompts1150,
ggsnaddress,
servicereasonreturncode,
lstofmessagebodies,
recordextension,
expires,
event,
lst1accessnetworkinfo AS Lst1AccessNetworkInfo,
lst1accessdomain,
lst1accesstype,
lst1locationinfotype,
lst1changetime,
lst1changetimenormalized,
lst2accessnetworkinfo,
lst2accessdomain,
lst2accesstype,
lst2locationinfotype,
lst2changetime,
lst2changetimenormalized,
lst3accessnetworkinfo,
lst3accessdomain,
lst3accesstype,
lst3locationinfotype,
lst3changetime,
lst3changetimenormalized,
lst4accessnetworkinfo,
lst4accessdomain,
lst4accesstype,
lst4locationinfotype,
lst4changetime,
lst4changetimenormalized,
lst5accessnetworkinfo,
lst5accessdomain,
lst5accesstype,
lst5locationinfotype,
lst5changetime,
lst5changetimenormalized,
lst6accessnetworkinfo,
lst6accessdomain,
lst6accesstype,
lst6locationinfotype,
lst6changetime,
lst6changetimenormalized,
lst7accessnetworkinfo,
lst7accessdomain,
lst7accesstype,
lst7locationinfotype,
lst7changetime,
lst7changetimenormalized,
lst8accessnetworkinfo,
lst8accessdomain,
lst8accesstype,
lst8locationinfotype,
lst8changetime,
lst8changetimenormalized,
lst9accessnetworkinfo,
lst9accessdomain,
lst9accesstype,
lst9locationinfotype,
lst9changetime,
lst9changetimenormalized,
lst10accessnetworkinfo,
lst10accessdomain,
lst10accesstype,
lst10locationinfotype,
lst10changetime,
lst10changetimenormalized,
servicecontextid,
--Phase 2 changes starts
NULL AS SubscriberE164_orig,
--Phase 2 changes ends
subscribere164,
subscriberno,
imsi,
imei,
subsipuri,
nai,
subprivate,
subservedpartydeviceid,
--Phase 2 changes starts
NULL AS OtherPartyAddress_orig,
--Phase 2 changes ends
NULL AS OtherPartyAddress,
NULL AS PhoneType,
NULL AS ServiceProvider,
NULL AS HG_GROUP,
--Phase 2 changes starts
NULL AS End_User_SubsID_orig,
NULL AS End_User_SubsID_CTN,
NULL AS End_User_SubsID_Dom_fl,
NULL AS End_User_SubsID_Dom_ty,
--Phase 2 changes ends	
lstofearlysdpmediacmpnts,
imscommserviceidentifier,
numberportabilityrouting,
carrierselectrouting,
sessionpriority,
--Phase 2 changes starts
NULL AS RequestedPartyAddress_orig,
--Phase 2 changes ends
requestedpartyaddress,
lstofcalledassertedid,
mmtelinfoanalyzedcalltype,
mtlinfocalledasrtidprsntsts,
mtlinfocllgprtyaddrprsntsts,
mmtelinfoconferenceid,
mmtelinfodialaroundindctr,
mmtelinforelatedicid,
mmtelsplmtrysrvcinfo1id,
mmtelsplmtrysrvcinfo1act,
--Phase 2 changes starts
NULL AS MMTelSplmtrySrvcInfo1Redir_orig,
--Phase 2 changes ends
mmtelsplmtrysrvcinfo1redir,
mmtelsplmtrysrvcinfo2id,
mmtelsplmtrysrvcinfo2act,
--Phase 2 changes starts
NULL AS MMTelSplmtrySrvcInfo2Redir_orig,
--Phase 2 changes ends	
mmtelsplmtrysrvcinfo2redir,
mmtelinfosplmtrysrvcinfo3,
NULL AS MMTelSplmtrySrvcInfo3ID,
NULL AS MMTelSplmtrySrvcInfo3Act,
--Phase 2 changes starts
NULL AS MMTelSplmtrySrvcInfo3Redir_orig,
--Phase 2 changes ends	
NULL AS MMTelSplmtrySrvcInfo3Redir,
mmtelinfosplmtrysrvcinfo4,
NULL AS MMTelSplmtrySrvcInfo4ID,
NULL AS MMTelSplmtrySrvcInfo4Act,
--Phase 2 changes starts
NULL AS MMTelSplmtrySrvcInfo4Redir_orig,
--Phase 2 changes ends	
NULL AS MMTelSplmtrySrvcInfo4Redir,
mmtelinfosplmtrysrvcinfo5,
NULL AS MMTelSplmtrySrvcInfo5ID,
NULL AS MMTelSplmtrySrvcInfo5Act,
--Phase 2 changes starts
NULL AS MMTelSplmtrySrvcInfo5Redir_orig,
--Phase 2 changes ends	
NULL AS MMTelSplmtrySrvcInfo5Redir,
mmtelinfosplmtrysrvcinfo6,
mmtelinfosplmtrysrvcinfo7,
mmtelinfosplmtrysrvcinfo8,
mmtelinfosplmtrysrvcinfo9,
mmtelinfosplmtrysrvcinfo10,
mobilestationroamingnumber,
teleservicecode,
tariffclass,
pvisitednetworkid,
--Phase 2 changes starts
NULL AS networkCallType,
NULL AS carrierIdCode,
--Phase 2 changes ends	
tr_ban,
tr_account_sub_type,
tr_company_code,
tr_franchise_cd,
--Phase 2 changes starts
NULL AS tr_product_type,
--Phase 2 changes ends
tr_ba_account_type,
NULL AS TR_Sub_ALS_ind,
NULL AS TR_Sub_oth_party_ALS_ind,
NULL AS TR_Re_dir1_ALS_ind,
NULL AS TR_Sub_ind_gen,
--Phase 2 changes starts
NULL AS WL_BAN,
NULL AS WL_ACCOUNT_SUB_TYPE,
NULL AS WL_COMPANY_CODE,
NULL AS WL_FRANCHISE_CD,
NULL AS WL_PRODUCT_TYPE,
NULL AS WL_BA_ACCOUNT_TYPE,      
-----------
NULL AS WL_SUB_ALS_IND,
NULL AS WL_SUB_OTH_PARTY_ALS_IND,
NULL AS WL_RE_DIR1_ALS_IND,
--Phase 2 changes ends
'IMM' AS CDR_type
FROM cdrdm.FACT_IMM_CDR
WHERE SIPMETHOD ='INVITE' 
AND RoleOfNode = 0 AND MMTelSplmtrySrvcInfo1ID NOT IN ('901','902','903','904','905','906','907','908','909','910','911',
'912','913','0','1','2','3','4','5','10','11','12','13','14','15','20','30','31');

CREATE VIEW IF NOT EXISTS v_MO_LD_UCC_temp2 AS
SELECT a.*,
b.serve_sid, -- > ROUTE table join with UCC/IMM
SUBSTR(a.calledpartyaddress, 1, 1) AS intl_ind,
IF(SUBSTR(a.calledpartyaddress, 1, 1) = 1, SUBSTR(a.calledpartyaddress,2,3), NULL) AS CDR_NPA,
IF(SUBSTR(a.calledpartyaddress, 1, 1) = 1, SUBSTR(a.calledpartyaddress,5,3), NULL) AS CDR_NXX
FROM ext_cdrdm.v_MO_LD_UCC_temp1 a
LEFT OUTER JOIN ela_v21.route_gg b
ON
--Phase 2 changes starts - modified logic
--UPPER(LPAD(SUBSTR(a.Lst1AccessNetworkInfo, INSTR(REVERSE(a.Lst1AccessNetworkInfo),'-'), LENGTH(a.Lst1AccessNetworkInfo)),8,'0')) = 
UPPER(LPAD(SUBSTR(a.Lst1AccessNetworkInfo, LENGTH(a.Lst1AccessNetworkInfo)- INSTR(REVERSE(a.Lst1AccessNetworkInfo),'-')+2, 8),8,'0')) =
UPPER(CASE WHEN b.switch_id = 'LTE' THEN lpad(b.route_id,8,'0') WHEN b.switch_id = '3G' THEN lpad(conv(b.route_id,10,16),8,'0') END);
--Phase 2 changes ends

CREATE VIEW IF NOT EXISTS v_MO_LD_UCC_temp3 AS
SELECT a.*, b.*
FROM ext_cdrdm.v_MO_LD_UCC_temp2 a
LEFT OUTER JOIN ext_cdrdm.v_MO_LD_UCC_NPA_NXX_STATE_MPS b
ON a.CDR_NPA = b.NPA AND a.CDR_NXX = b.NXX;

CREATE VIEW IF NOT EXISTS v_MO_LD_UCC_temp4 AS
SELECT a.*, b.*
FROM ext_cdrdm.v_MO_LD_UCC_temp3 a
LEFT OUTER JOIN ext_cdrdm.v_MO_LD_UCC_LOCAL_CALLING_AREA b
ON a.serve_sid = b.sid AND a.V_Coordinates = b.Vertical_Coordinate AND a.H_Coordinates = b.Horizontal_Cordinate;

USE cdrdm;

DROP VIEW IF EXISTS v_MO_LD_UCC;

CREATE VIEW IF NOT EXISTS v_MO_LD_UCC AS
SELECT c.*,
(CASE WHEN c.Lst1AccessNetworkInfo = '' or c.Lst1AccessNetworkInfo IS NULL THEN 'No_cellIID'
WHEN c.INTL_IND <> '1' THEN 'INTL'
WHEN c.calledpartyaddress = '' or c.calledpartyaddress IS NULL THEN 'No_CalledNo'
WHEN c.LCA_FLAG IS NOT NULL AND c.Lst1AccessNetworkInfo IS NOT NULL AND c.Lst1AccessNetworkInfo <> '' THEN 'LOCAL'
WHEN c.LCA_FLAG IS NULL AND c.COUNTRY_CODE = 'CAN' AND Lst1AccessNetworkInfo IS NOT NULL AND c.Lst1AccessNetworkInfo <> '' THEN 'LD_CAN'
WHEN c.LCA_FLAG IS NULL AND c.COUNTRY_CODE = 'USA' AND Lst1AccessNetworkInfo IS NOT NULL AND c.Lst1AccessNetworkInfo <> '' THEN 'LD_US'
ELSE 0 END) AS ld_ind
FROM ext_cdrdm.v_MO_LD_UCC_temp4 c;