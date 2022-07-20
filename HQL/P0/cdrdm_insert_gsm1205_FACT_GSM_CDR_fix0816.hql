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

DROP TABLE IF EXISTS ext_cdrdm.gsm1205_temp1;
DROP TABLE IF EXISTS ext_cdrdm.gsm1205_temp2;

CREATE TABLE IF NOT EXISTS ext_cdrdm.gsm1205_temp1 AS 
SELECT 
CASE WHEN chargednumber3_t IN ('0','') THEN TRIM(COALESCE((chargenumber3),'')) ELSE TRIM(chargednumber3_t) END AS cleansedCharged,
CASE WHEN callingnumber3_t IN ('0','') THEN TRIM(COALESCE((callingnumber3),'')) ELSE TRIM(callingnumber3_t) END AS cleansedCalling,
CASE WHEN LENGTH((CASE WHEN SUBSTR(calledpartynumber,1,5) IN ('12198','12199') THEN callednumber6_t
ELSE CASE WHEN SUBSTR(callednumber3_t,1,16) IN ('0','') THEN SUBSTR(COALESCE((callednumber3),''),1,16)
ELSE SUBSTR(callednumber3_t,1,16) END
END))>10 THEN SUBSTR((CASE WHEN SUBSTR(calledpartynumber,1,5) IN ('12198','12199') THEN callednumber6_t
ELSE CASE WHEN SUBSTR(callednumber3_t,1,16) IN('0','') THEN SUBSTR(COALESCE((callednumber3),''),1,16)
ELSE SUBSTR(callednumber3_t,1,16) END
END),(LENGTH((CASE WHEN SUBSTR(calledpartynumber,1,5) IN ('12198','12199') THEN callednumber6_t
ELSE CASE WHEN SUBSTR(callednumber3_t,1,16) IN ('0','') THEN SUBSTR(COALESCE((callednumber3),''),1,16)
ELSE SUBSTR(callednumber3_t,1,16) END
END))+1-10))
ELSE (CASE WHEN SUBSTR(calledpartynumber,1,5) IN ('12198','12199') THEN callednumber6_t
ELSE CASE WHEN SUBSTR(callednumber3_t,1,16) IN ('0','') THEN SUBSTR(COALESCE((callednumber3),''),1,16)
ELSE SUBSTR(callednumber3_t,1,16) END END) END AS cleansedcalled,
CASE WHEN SUBSTR(originalcalled3_t,1,16) IN ('0','') THEN SUBSTR(COALESCE(originalcalled3 ,''),1,16) ELSE SUBSTR(originalcalled3_t,1,16) END AS cleansedOriginal,
CASE WHEN LENGTH(REDIRECTINGNUMBER3_t) = 10 THEN REDIRECTINGNUMBER3_t 
ELSE CASE WHEN SUBSTR(redirectingnumber,1,4) IN ('14BB') THEN 
CASE WHEN LENGTH(REDIRECTINGNUMBER7_t) < 10 THEN REDIRECTINGNUMBER7_t END 
WHEN SUBSTR(redirectingnumber,1,4) IN ('14B3') THEN 
CASE WHEN LENGTH(REDIRECTINGNUMBER8_t) < 10 THEN REDIRECTINGNUMBER8_t END        
WHEN SUBSTR(redirectingnumber,1,4) IN ('14AA') THEN 
CASE WHEN LENGTH(REDIRECTINGNUMBER11_t) < 10 THEN REDIRECTINGNUMBER11_t ELSE REDIRECTINGNUMBER11_t END 
ELSE CASE WHEN SUBSTR(REDIRECTINGNUMBER3_t,1,16) IN ('0','') THEN SUBSTR(COALESCE(REDIRECTINGNUMBER3,''),1,16)
ELSE SUBSTR(REDIRECTINGNUMBER3_t,1,16) END END END AS CLEANSEDREDIRECTING,
--Phone Number fields:
callingpartynumber,
calledpartynumber,
originalcallednumber,
RedirectingNumber,
ChargeNumber,
MobileStationRoamingNumber,
--Call Detail fields:
CONV(recordsequencenumber,16,10) AS Recordsequencenumber,
IMSI,
IMEI,
CONCAT('20', SUBSTR(dateforstartofcharge,1,2), '-', SUBSTR(dateforstartofcharge,3,2), '-', SUBSTR(dateforstartofcharge,5,2)) AS DATE_FOR_START_OF_CHARGE,
TRIM(timeforstartofcharge) AS Time_for_Start_of_Charge,
chargeableduration,
IF(timefromregisterseizuretostartofcharging <> '',concat(lpad(CONV(substr(timefromregisterseizuretostartofcharging,1,2),16,10),2,'0'),lpad(CONV(substr(timefromregisterseizuretostartofcharging,3,2),16,10),2,'0'),lpad(CONV(substr(timefromregisterseizuretostartofcharging,5,2),16,10),2,'0')),'') AS timefortcseizure, 
--Network fields:
incomingroute,
outgoingroute,
CellIDForFirstCell,
CellIDForLastCell,
exchangeidentity,
switchIdentity,
--Error fields:
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,1,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,1,1))) THEN SUBSTR(partialoutputrecnum,1,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,2,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,2,1))) THEN SUBSTR(partialoutputrecnum,2,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,3,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,3,1))) THEN SUBSTR(partialoutputrecnum,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,4,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,4,1))) THEN SUBSTR(partialoutputrecnum,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,5,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,5,1))) THEN SUBSTR(partialoutputrecnum,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,6,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,6,1))) THEN SUBSTR(partialoutputrecnum,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,7,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,7,1))) THEN SUBSTR(partialoutputrecnum,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,8,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,8,1))) THEN SUBSTR(partialoutputrecnum,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,9,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,9,1))) THEN SUBSTR(partialoutputrecnum,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,10,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,10,1))) THEN SUBSTR(partialoutputrecnum,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,11,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,11,1))) THEN SUBSTR(partialoutputrecnum,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,12,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,12,1))) THEN SUBSTR(partialoutputrecnum,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,13,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,13,1))) THEN SUBSTR(partialoutputrecnum,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,14,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,14,1))) THEN SUBSTR(partialoutputrecnum,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,15,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,15,1))) THEN SUBSTR(partialoutputrecnum,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,16,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,16,1))) THEN SUBSTR(partialoutputrecnum,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,17,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,17,1))) THEN SUBSTR(partialoutputrecnum,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,18,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,18,1))) THEN SUBSTR(partialoutputrecnum,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,19,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,19,1))) THEN SUBSTR(partialoutputrecnum,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,20,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,20,1))) THEN SUBSTR(partialoutputrecnum,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,21,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,21,1))) THEN SUBSTR(partialoutputrecnum,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,22,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,22,1))) THEN SUBSTR(partialoutputrecnum,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,23,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,23,1))) THEN SUBSTR(partialoutputrecnum,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,24,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,24,1))) THEN SUBSTR(partialoutputrecnum,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,25,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,25,1))) THEN SUBSTR(partialoutputrecnum,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,26,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,26,1))) THEN SUBSTR(partialoutputrecnum,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,27,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,27,1))) THEN SUBSTR(partialoutputrecnum,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,28,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,28,1))) THEN SUBSTR(partialoutputrecnum,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,29,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,29,1))) THEN SUBSTR(partialoutputrecnum,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(partialoutputrecnum,30,1))) = HEX(LOWER(SUBSTR(partialoutputrecnum,30,1))) THEN SUBSTR(partialoutputrecnum,30,1) ELSE ''  END ) AS partialoutputrecnum,                                                                                                    -- 026
FaultCode,
EOSInfo,
CASE WHEN TRIM(disconnectingparty) = 'callingPartyRelease' THEN 0
WHEN TRIM(disconnectingparty) = 'calledPartyRelease' THEN 1
WHEN TRIM(disconnectingparty) = 'networkRelease' THEN 2
WHEN TRIM(disconnectingparty) = '' THEN NULL ELSE NULL END AS disconnectingparty,
internalCauseAndLoc, 
trafficactivitycode,
CASE WHEN TRIM(ChargedParty) = 'chargingOfCallingSubscriber' THEN 0
WHEN TRIM(ChargedParty) = 'chargingOfCalledSubscriber' THEN 1
WHEN TRIM(ChargedParty) = 'noCharging' THEN 2 ELSE NULL END AS ChargedParty,
regionDependentChargingOrigin,
CASE WHEN TRIM(multimediacall) = '' THEN -1 ELSE CASE WHEN  multimediacall IS NULL THEN -1 ELSE multimediacall END END AS multimediacall,
CASE WHEN TRIM(teleservicecode) = '' THEN  NULL ELSE CASE WHEN  teleservicecode='D1' THEN  '209' ELSE teleservicecode END END AS teleservicecode,
tariffclass,
firstlocationinformationextension,
subscriptionType,
CASE WHEN TRIM(SRVCCIndicator) = 'e-UTRAN' THEN 0
WHEN TRIM(SRVCCIndicator) = 'uTRAN-HSPA' THEN 1
WHEN TRIM(SRVCCIndicator) = 'gERAN' THEN 2
WHEN TRIM(SRVCCIndicator) = 'uTRAN' THEN 3 ELSE NULL END AS SRVCCIndicator,
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
FROM (
SELECT
callingpartynumber,
calledpartynumber,
originalcallednumber,
CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END AS redirectingnumber,
chargenumber,
mobilestationroamingnumber,
recordsequencenumber,
imsi,
imei,
dateforstartofcharge,
timeforstartofcharge,
chargeableduration,
timefromregisterseizuretostartofcharging,
incomingroute,
outgoingroute,
cellidforfirstcell,
cellidforlastcell,
exchangeidentity,
switchIdentity,
partialoutputrecnum,
faultcode,
eosinfo,
disconnectingparty,
internalcauseandloc,
trafficactivitycode,
chargedparty,
regiondependentchargingorigin,
multimediacall,
teleservicecode,
tariffclass,
firstlocationinformationextension,
subscriptionType,
SRVCCIndicator,
--messageTypeIndicator,
--mSCAddress,
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
file_date,     

--PREDEFINE
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,3,1))) = HEX(LOWER(SUBSTR(ChargeNumber,3,1))) THEN SUBSTR( ChargeNumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,4,1))) = HEX(LOWER(SUBSTR(ChargeNumber,4,1))) THEN SUBSTR( ChargeNumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,5,1))) = HEX(LOWER(SUBSTR(ChargeNumber,5,1))) THEN SUBSTR( ChargeNumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,6,1))) = HEX(LOWER(SUBSTR(ChargeNumber,6,1))) THEN SUBSTR( ChargeNumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,7,1))) = HEX(LOWER(SUBSTR(ChargeNumber,7,1))) THEN SUBSTR( ChargeNumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,8,1))) = HEX(LOWER(SUBSTR(ChargeNumber,8,1))) THEN SUBSTR( ChargeNumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,9,1))) = HEX(LOWER(SUBSTR(ChargeNumber,9,1))) THEN SUBSTR( ChargeNumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,10,1))) = HEX(LOWER(SUBSTR(ChargeNumber,10,1))) THEN SUBSTR( ChargeNumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,11,1))) = HEX(LOWER(SUBSTR(ChargeNumber,11,1))) THEN SUBSTR( ChargeNumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,12,1))) = HEX(LOWER(SUBSTR(ChargeNumber,12,1))) THEN SUBSTR( ChargeNumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,13,1))) = HEX(LOWER(SUBSTR(ChargeNumber,13,1))) THEN SUBSTR( ChargeNumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,14,1))) = HEX(LOWER(SUBSTR(ChargeNumber,14,1))) THEN SUBSTR( ChargeNumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,15,1))) = HEX(LOWER(SUBSTR(ChargeNumber,15,1))) THEN SUBSTR( ChargeNumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,16,1))) = HEX(LOWER(SUBSTR(ChargeNumber,16,1))) THEN SUBSTR( ChargeNumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,17,1))) = HEX(LOWER(SUBSTR(ChargeNumber,17,1))) THEN SUBSTR( ChargeNumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,18,1))) = HEX(LOWER(SUBSTR(ChargeNumber,18,1))) THEN SUBSTR( ChargeNumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,19,1))) = HEX(LOWER(SUBSTR(ChargeNumber,19,1))) THEN SUBSTR( ChargeNumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,20,1))) = HEX(LOWER(SUBSTR(ChargeNumber,20,1))) THEN SUBSTR( ChargeNumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,21,1))) = HEX(LOWER(SUBSTR(ChargeNumber,21,1))) THEN SUBSTR( ChargeNumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,22,1))) = HEX(LOWER(SUBSTR(ChargeNumber,22,1))) THEN SUBSTR( ChargeNumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,23,1))) = HEX(LOWER(SUBSTR(ChargeNumber,23,1))) THEN SUBSTR( ChargeNumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,24,1))) = HEX(LOWER(SUBSTR(ChargeNumber,24,1))) THEN SUBSTR( ChargeNumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,25,1))) = HEX(LOWER(SUBSTR(ChargeNumber,25,1))) THEN SUBSTR( ChargeNumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,26,1))) = HEX(LOWER(SUBSTR(ChargeNumber,26,1))) THEN SUBSTR( ChargeNumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,27,1))) = HEX(LOWER(SUBSTR(ChargeNumber,27,1))) THEN SUBSTR( ChargeNumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,28,1))) = HEX(LOWER(SUBSTR(ChargeNumber,28,1))) THEN SUBSTR( ChargeNumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,29,1))) = HEX(LOWER(SUBSTR(ChargeNumber,29,1))) THEN SUBSTR( ChargeNumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,30,1))) = HEX(LOWER(SUBSTR(ChargeNumber,30,1))) THEN SUBSTR( ChargeNumber,30,1) ELSE ''  END )
AS chargenumber3,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,3,1))) = HEX(LOWER(SUBSTR(callingpartynumber,3,1))) THEN SUBSTR(callingpartynumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,4,1))) = HEX(LOWER(SUBSTR(callingpartynumber,4,1))) THEN SUBSTR(callingpartynumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,5,1))) = HEX(LOWER(SUBSTR(callingpartynumber,5,1))) THEN SUBSTR(callingpartynumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,6,1))) = HEX(LOWER(SUBSTR(callingpartynumber,6,1))) THEN SUBSTR(callingpartynumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,7,1))) = HEX(LOWER(SUBSTR(callingpartynumber,7,1))) THEN SUBSTR(callingpartynumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,8,1))) = HEX(LOWER(SUBSTR(callingpartynumber,8,1))) THEN SUBSTR(callingpartynumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,9,1))) = HEX(LOWER(SUBSTR(callingpartynumber,9,1))) THEN SUBSTR(callingpartynumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,10,1))) = HEX(LOWER(SUBSTR(callingpartynumber,10,1))) THEN SUBSTR(callingpartynumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,11,1))) = HEX(LOWER(SUBSTR(callingpartynumber,11,1))) THEN SUBSTR(callingpartynumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,12,1))) = HEX(LOWER(SUBSTR(callingpartynumber,12,1))) THEN SUBSTR(callingpartynumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,13,1))) = HEX(LOWER(SUBSTR(callingpartynumber,13,1))) THEN SUBSTR(callingpartynumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,14,1))) = HEX(LOWER(SUBSTR(callingpartynumber,14,1))) THEN SUBSTR(callingpartynumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,15,1))) = HEX(LOWER(SUBSTR(callingpartynumber,15,1))) THEN SUBSTR(callingpartynumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,16,1))) = HEX(LOWER(SUBSTR(callingpartynumber,16,1))) THEN SUBSTR(callingpartynumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,17,1))) = HEX(LOWER(SUBSTR(callingpartynumber,17,1))) THEN SUBSTR(callingpartynumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,18,1))) = HEX(LOWER(SUBSTR(callingpartynumber,18,1))) THEN SUBSTR(callingpartynumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,19,1))) = HEX(LOWER(SUBSTR(callingpartynumber,19,1))) THEN SUBSTR(callingpartynumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,20,1))) = HEX(LOWER(SUBSTR(callingpartynumber,20,1))) THEN SUBSTR(callingpartynumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,21,1))) = HEX(LOWER(SUBSTR(callingpartynumber,21,1))) THEN SUBSTR(callingpartynumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,22,1))) = HEX(LOWER(SUBSTR(callingpartynumber,22,1))) THEN SUBSTR(callingpartynumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,23,1))) = HEX(LOWER(SUBSTR(callingpartynumber,23,1))) THEN SUBSTR(callingpartynumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,24,1))) = HEX(LOWER(SUBSTR(callingpartynumber,24,1))) THEN SUBSTR(callingpartynumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,25,1))) = HEX(LOWER(SUBSTR(callingpartynumber,25,1))) THEN SUBSTR(callingpartynumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,26,1))) = HEX(LOWER(SUBSTR(callingpartynumber,26,1))) THEN SUBSTR(callingpartynumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,27,1))) = HEX(LOWER(SUBSTR(callingpartynumber,27,1))) THEN SUBSTR(callingpartynumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,28,1))) = HEX(LOWER(SUBSTR(callingpartynumber,28,1))) THEN SUBSTR(callingpartynumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,29,1))) = HEX(LOWER(SUBSTR(callingpartynumber,29,1))) THEN SUBSTR(callingpartynumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,30,1))) = HEX(LOWER(SUBSTR(callingpartynumber,30,1))) THEN SUBSTR(callingpartynumber,30,1) ELSE ''  END )
AS callingnumber3,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,6,1))) = HEX(LOWER(SUBSTR(calledpartynumber,6,1))) THEN SUBSTR(calledpartynumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,7,1))) = HEX(LOWER(SUBSTR(calledpartynumber,7,1))) THEN SUBSTR(calledpartynumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,8,1))) = HEX(LOWER(SUBSTR(calledpartynumber,8,1))) THEN SUBSTR(calledpartynumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,9,1))) = HEX(LOWER(SUBSTR(calledpartynumber,9,1))) THEN SUBSTR(calledpartynumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,10,1))) = HEX(LOWER(SUBSTR(calledpartynumber,10,1))) THEN SUBSTR(calledpartynumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,11,1))) = HEX(LOWER(SUBSTR(calledpartynumber,11,1))) THEN SUBSTR(calledpartynumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,12,1))) = HEX(LOWER(SUBSTR(calledpartynumber,12,1))) THEN SUBSTR(calledpartynumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,13,1))) = HEX(LOWER(SUBSTR(calledpartynumber,13,1))) THEN SUBSTR(calledpartynumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,14,1))) = HEX(LOWER(SUBSTR(calledpartynumber,14,1))) THEN SUBSTR(calledpartynumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,15,1))) = HEX(LOWER(SUBSTR(calledpartynumber,15,1))) THEN SUBSTR(calledpartynumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,16,1))) = HEX(LOWER(SUBSTR(calledpartynumber,16,1))) THEN SUBSTR(calledpartynumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,17,1))) = HEX(LOWER(SUBSTR(calledpartynumber,17,1))) THEN SUBSTR(calledpartynumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,18,1))) = HEX(LOWER(SUBSTR(calledpartynumber,18,1))) THEN SUBSTR(calledpartynumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,19,1))) = HEX(LOWER(SUBSTR(calledpartynumber,19,1))) THEN SUBSTR(calledpartynumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,20,1))) = HEX(LOWER(SUBSTR(calledpartynumber,20,1))) THEN SUBSTR(calledpartynumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,21,1))) = HEX(LOWER(SUBSTR(calledpartynumber,21,1))) THEN SUBSTR(calledpartynumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,22,1))) = HEX(LOWER(SUBSTR(calledpartynumber,22,1))) THEN SUBSTR(calledpartynumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,23,1))) = HEX(LOWER(SUBSTR(calledpartynumber,23,1))) THEN SUBSTR(calledpartynumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,24,1))) = HEX(LOWER(SUBSTR(calledpartynumber,24,1))) THEN SUBSTR(calledpartynumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,25,1))) = HEX(LOWER(SUBSTR(calledpartynumber,25,1))) THEN SUBSTR(calledpartynumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,26,1))) = HEX(LOWER(SUBSTR(calledpartynumber,26,1))) THEN SUBSTR(calledpartynumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,27,1))) = HEX(LOWER(SUBSTR(calledpartynumber,27,1))) THEN SUBSTR(calledpartynumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,28,1))) = HEX(LOWER(SUBSTR(calledpartynumber,28,1))) THEN SUBSTR(calledpartynumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,29,1))) = HEX(LOWER(SUBSTR(calledpartynumber,29,1))) THEN SUBSTR(calledpartynumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,30,1))) = HEX(LOWER(SUBSTR(calledpartynumber,30,1))) THEN SUBSTR(calledpartynumber,30,1) ELSE ''  END ) 
AS callednumber6,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,3,1))) = HEX(LOWER(SUBSTR(calledpartynumber,3,1))) THEN SUBSTR(calledpartynumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,4,1))) = HEX(LOWER(SUBSTR(calledpartynumber,4,1))) THEN SUBSTR(calledpartynumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,5,1))) = HEX(LOWER(SUBSTR(calledpartynumber,5,1))) THEN SUBSTR(calledpartynumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,6,1))) = HEX(LOWER(SUBSTR(calledpartynumber,6,1))) THEN SUBSTR(calledpartynumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,7,1))) = HEX(LOWER(SUBSTR(calledpartynumber,7,1))) THEN SUBSTR(calledpartynumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,8,1))) = HEX(LOWER(SUBSTR(calledpartynumber,8,1))) THEN SUBSTR(calledpartynumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,9,1))) = HEX(LOWER(SUBSTR(calledpartynumber,9,1))) THEN SUBSTR(calledpartynumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,10,1))) = HEX(LOWER(SUBSTR(calledpartynumber,10,1))) THEN SUBSTR(calledpartynumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,11,1))) = HEX(LOWER(SUBSTR(calledpartynumber,11,1))) THEN SUBSTR(calledpartynumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,12,1))) = HEX(LOWER(SUBSTR(calledpartynumber,12,1))) THEN SUBSTR(calledpartynumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,13,1))) = HEX(LOWER(SUBSTR(calledpartynumber,13,1))) THEN SUBSTR(calledpartynumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,14,1))) = HEX(LOWER(SUBSTR(calledpartynumber,14,1))) THEN SUBSTR(calledpartynumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,15,1))) = HEX(LOWER(SUBSTR(calledpartynumber,15,1))) THEN SUBSTR(calledpartynumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,16,1))) = HEX(LOWER(SUBSTR(calledpartynumber,16,1))) THEN SUBSTR(calledpartynumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,17,1))) = HEX(LOWER(SUBSTR(calledpartynumber,17,1))) THEN SUBSTR(calledpartynumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,18,1))) = HEX(LOWER(SUBSTR(calledpartynumber,18,1))) THEN SUBSTR(calledpartynumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,19,1))) = HEX(LOWER(SUBSTR(calledpartynumber,19,1))) THEN SUBSTR(calledpartynumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,20,1))) = HEX(LOWER(SUBSTR(calledpartynumber,20,1))) THEN SUBSTR(calledpartynumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,21,1))) = HEX(LOWER(SUBSTR(calledpartynumber,21,1))) THEN SUBSTR(calledpartynumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,22,1))) = HEX(LOWER(SUBSTR(calledpartynumber,22,1))) THEN SUBSTR(calledpartynumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,23,1))) = HEX(LOWER(SUBSTR(calledpartynumber,23,1))) THEN SUBSTR(calledpartynumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,24,1))) = HEX(LOWER(SUBSTR(calledpartynumber,24,1))) THEN SUBSTR(calledpartynumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,25,1))) = HEX(LOWER(SUBSTR(calledpartynumber,25,1))) THEN SUBSTR(calledpartynumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,26,1))) = HEX(LOWER(SUBSTR(calledpartynumber,26,1))) THEN SUBSTR(calledpartynumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,27,1))) = HEX(LOWER(SUBSTR(calledpartynumber,27,1))) THEN SUBSTR(calledpartynumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,28,1))) = HEX(LOWER(SUBSTR(calledpartynumber,28,1))) THEN SUBSTR(calledpartynumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,29,1))) = HEX(LOWER(SUBSTR(calledpartynumber,29,1))) THEN SUBSTR(calledpartynumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,30,1))) = HEX(LOWER(SUBSTR(calledpartynumber,30,1))) THEN SUBSTR(calledpartynumber,30,1) ELSE ''  END )
AS callednumber3,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,3,1))) = HEX(LOWER(SUBSTR(originalcallednumber,3,1))) THEN SUBSTR(originalcallednumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,4,1))) = HEX(LOWER(SUBSTR(originalcallednumber,4,1))) THEN SUBSTR(originalcallednumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,5,1))) = HEX(LOWER(SUBSTR(originalcallednumber,5,1))) THEN SUBSTR(originalcallednumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,6,1))) = HEX(LOWER(SUBSTR(originalcallednumber,6,1))) THEN SUBSTR(originalcallednumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,7,1))) = HEX(LOWER(SUBSTR(originalcallednumber,7,1))) THEN SUBSTR(originalcallednumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,8,1))) = HEX(LOWER(SUBSTR(originalcallednumber,8,1))) THEN SUBSTR(originalcallednumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,9,1))) = HEX(LOWER(SUBSTR(originalcallednumber,9,1))) THEN SUBSTR(originalcallednumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,10,1))) = HEX(LOWER(SUBSTR(originalcallednumber,10,1))) THEN SUBSTR(originalcallednumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,11,1))) = HEX(LOWER(SUBSTR(originalcallednumber,11,1))) THEN SUBSTR(originalcallednumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,12,1))) = HEX(LOWER(SUBSTR(originalcallednumber,12,1))) THEN SUBSTR(originalcallednumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,13,1))) = HEX(LOWER(SUBSTR(originalcallednumber,13,1))) THEN SUBSTR(originalcallednumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,14,1))) = HEX(LOWER(SUBSTR(originalcallednumber,14,1))) THEN SUBSTR(originalcallednumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,15,1))) = HEX(LOWER(SUBSTR(originalcallednumber,15,1))) THEN SUBSTR(originalcallednumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,16,1))) = HEX(LOWER(SUBSTR(originalcallednumber,16,1))) THEN SUBSTR(originalcallednumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,17,1))) = HEX(LOWER(SUBSTR(originalcallednumber,17,1))) THEN SUBSTR(originalcallednumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,18,1))) = HEX(LOWER(SUBSTR(originalcallednumber,18,1))) THEN SUBSTR(originalcallednumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,19,1))) = HEX(LOWER(SUBSTR(originalcallednumber,19,1))) THEN SUBSTR(originalcallednumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,20,1))) = HEX(LOWER(SUBSTR(originalcallednumber,20,1))) THEN SUBSTR(originalcallednumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,21,1))) = HEX(LOWER(SUBSTR(originalcallednumber,21,1))) THEN SUBSTR(originalcallednumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,22,1))) = HEX(LOWER(SUBSTR(originalcallednumber,22,1))) THEN SUBSTR(originalcallednumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,23,1))) = HEX(LOWER(SUBSTR(originalcallednumber,23,1))) THEN SUBSTR(originalcallednumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,24,1))) = HEX(LOWER(SUBSTR(originalcallednumber,24,1))) THEN SUBSTR(originalcallednumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,25,1))) = HEX(LOWER(SUBSTR(originalcallednumber,25,1))) THEN SUBSTR(originalcallednumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,26,1))) = HEX(LOWER(SUBSTR(originalcallednumber,26,1))) THEN SUBSTR(originalcallednumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,27,1))) = HEX(LOWER(SUBSTR(originalcallednumber,27,1))) THEN SUBSTR(originalcallednumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,28,1))) = HEX(LOWER(SUBSTR(originalcallednumber,28,1))) THEN SUBSTR(originalcallednumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,29,1))) = HEX(LOWER(SUBSTR(originalcallednumber,29,1))) THEN SUBSTR(originalcallednumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,30,1))) = HEX(LOWER(SUBSTR(originalcallednumber,30,1))) THEN SUBSTR(originalcallednumber,30,1) ELSE ''  END )
AS originalcalled3,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,3,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,3,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,4,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,4,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,5,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,5,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,6,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,6,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END )
AS REDIRECTINGNUMBER3,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END )
AS REDIRECTINGNUMBER7,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END )
AS REDIRECTINGNUMBER8,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END )
AS REDIRECTINGNUMBER11,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,3,1))) = HEX(LOWER(SUBSTR(ChargeNumber,3,1))) THEN SUBSTR( ChargeNumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,4,1))) = HEX(LOWER(SUBSTR(ChargeNumber,4,1))) THEN SUBSTR( ChargeNumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,5,1))) = HEX(LOWER(SUBSTR(ChargeNumber,5,1))) THEN SUBSTR( ChargeNumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,6,1))) = HEX(LOWER(SUBSTR(ChargeNumber,6,1))) THEN SUBSTR( ChargeNumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,7,1))) = HEX(LOWER(SUBSTR(ChargeNumber,7,1))) THEN SUBSTR( ChargeNumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,8,1))) = HEX(LOWER(SUBSTR(ChargeNumber,8,1))) THEN SUBSTR( ChargeNumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,9,1))) = HEX(LOWER(SUBSTR(ChargeNumber,9,1))) THEN SUBSTR( ChargeNumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,10,1))) = HEX(LOWER(SUBSTR(ChargeNumber,10,1))) THEN SUBSTR( ChargeNumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,11,1))) = HEX(LOWER(SUBSTR(ChargeNumber,11,1))) THEN SUBSTR( ChargeNumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,12,1))) = HEX(LOWER(SUBSTR(ChargeNumber,12,1))) THEN SUBSTR( ChargeNumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,13,1))) = HEX(LOWER(SUBSTR(ChargeNumber,13,1))) THEN SUBSTR( ChargeNumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,14,1))) = HEX(LOWER(SUBSTR(ChargeNumber,14,1))) THEN SUBSTR( ChargeNumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,15,1))) = HEX(LOWER(SUBSTR(ChargeNumber,15,1))) THEN SUBSTR( ChargeNumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,16,1))) = HEX(LOWER(SUBSTR(ChargeNumber,16,1))) THEN SUBSTR( ChargeNumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,17,1))) = HEX(LOWER(SUBSTR(ChargeNumber,17,1))) THEN SUBSTR( ChargeNumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,18,1))) = HEX(LOWER(SUBSTR(ChargeNumber,18,1))) THEN SUBSTR( ChargeNumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,19,1))) = HEX(LOWER(SUBSTR(ChargeNumber,19,1))) THEN SUBSTR( ChargeNumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,20,1))) = HEX(LOWER(SUBSTR(ChargeNumber,20,1))) THEN SUBSTR( ChargeNumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,21,1))) = HEX(LOWER(SUBSTR(ChargeNumber,21,1))) THEN SUBSTR( ChargeNumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,22,1))) = HEX(LOWER(SUBSTR(ChargeNumber,22,1))) THEN SUBSTR( ChargeNumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,23,1))) = HEX(LOWER(SUBSTR(ChargeNumber,23,1))) THEN SUBSTR( ChargeNumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,24,1))) = HEX(LOWER(SUBSTR(ChargeNumber,24,1))) THEN SUBSTR( ChargeNumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,25,1))) = HEX(LOWER(SUBSTR(ChargeNumber,25,1))) THEN SUBSTR( ChargeNumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,26,1))) = HEX(LOWER(SUBSTR(ChargeNumber,26,1))) THEN SUBSTR( ChargeNumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,27,1))) = HEX(LOWER(SUBSTR(ChargeNumber,27,1))) THEN SUBSTR( ChargeNumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,28,1))) = HEX(LOWER(SUBSTR(ChargeNumber,28,1))) THEN SUBSTR( ChargeNumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,29,1))) = HEX(LOWER(SUBSTR(ChargeNumber,29,1))) THEN SUBSTR( ChargeNumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(ChargeNumber,30,1))) = HEX(LOWER(SUBSTR(ChargeNumber,30,1))) THEN SUBSTR( ChargeNumber,30,1) ELSE ''  END   )),' '),'^[01]*','') AS chargednumber3_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,3,1))) = HEX(LOWER(SUBSTR(callingpartynumber,3,1))) THEN SUBSTR(callingpartynumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,4,1))) = HEX(LOWER(SUBSTR(callingpartynumber,4,1))) THEN SUBSTR(callingpartynumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,5,1))) = HEX(LOWER(SUBSTR(callingpartynumber,5,1))) THEN SUBSTR(callingpartynumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,6,1))) = HEX(LOWER(SUBSTR(callingpartynumber,6,1))) THEN SUBSTR(callingpartynumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,7,1))) = HEX(LOWER(SUBSTR(callingpartynumber,7,1))) THEN SUBSTR(callingpartynumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,8,1))) = HEX(LOWER(SUBSTR(callingpartynumber,8,1))) THEN SUBSTR(callingpartynumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,9,1))) = HEX(LOWER(SUBSTR(callingpartynumber,9,1))) THEN SUBSTR(callingpartynumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,10,1))) = HEX(LOWER(SUBSTR(callingpartynumber,10,1))) THEN SUBSTR(callingpartynumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,11,1))) = HEX(LOWER(SUBSTR(callingpartynumber,11,1))) THEN SUBSTR(callingpartynumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,12,1))) = HEX(LOWER(SUBSTR(callingpartynumber,12,1))) THEN SUBSTR(callingpartynumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,13,1))) = HEX(LOWER(SUBSTR(callingpartynumber,13,1))) THEN SUBSTR(callingpartynumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,14,1))) = HEX(LOWER(SUBSTR(callingpartynumber,14,1))) THEN SUBSTR(callingpartynumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,15,1))) = HEX(LOWER(SUBSTR(callingpartynumber,15,1))) THEN SUBSTR(callingpartynumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,16,1))) = HEX(LOWER(SUBSTR(callingpartynumber,16,1))) THEN SUBSTR(callingpartynumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,17,1))) = HEX(LOWER(SUBSTR(callingpartynumber,17,1))) THEN SUBSTR(callingpartynumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,18,1))) = HEX(LOWER(SUBSTR(callingpartynumber,18,1))) THEN SUBSTR(callingpartynumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,19,1))) = HEX(LOWER(SUBSTR(callingpartynumber,19,1))) THEN SUBSTR(callingpartynumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,20,1))) = HEX(LOWER(SUBSTR(callingpartynumber,20,1))) THEN SUBSTR(callingpartynumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,21,1))) = HEX(LOWER(SUBSTR(callingpartynumber,21,1))) THEN SUBSTR(callingpartynumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,22,1))) = HEX(LOWER(SUBSTR(callingpartynumber,22,1))) THEN SUBSTR(callingpartynumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,23,1))) = HEX(LOWER(SUBSTR(callingpartynumber,23,1))) THEN SUBSTR(callingpartynumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,24,1))) = HEX(LOWER(SUBSTR(callingpartynumber,24,1))) THEN SUBSTR(callingpartynumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,25,1))) = HEX(LOWER(SUBSTR(callingpartynumber,25,1))) THEN SUBSTR(callingpartynumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,26,1))) = HEX(LOWER(SUBSTR(callingpartynumber,26,1))) THEN SUBSTR(callingpartynumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,27,1))) = HEX(LOWER(SUBSTR(callingpartynumber,27,1))) THEN SUBSTR(callingpartynumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,28,1))) = HEX(LOWER(SUBSTR(callingpartynumber,28,1))) THEN SUBSTR(callingpartynumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,29,1))) = HEX(LOWER(SUBSTR(callingpartynumber,29,1))) THEN SUBSTR(callingpartynumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(callingpartynumber,30,1))) = HEX(LOWER(SUBSTR(callingpartynumber,30,1))) THEN SUBSTR(callingpartynumber,30,1) ELSE ''  END )),' '),'^[01]*','') AS callingnumber3_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,6,1))) = HEX(LOWER(SUBSTR(calledpartynumber,6,1))) THEN SUBSTR(calledpartynumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,7,1))) = HEX(LOWER(SUBSTR(calledpartynumber,7,1))) THEN SUBSTR(calledpartynumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,8,1))) = HEX(LOWER(SUBSTR(calledpartynumber,8,1))) THEN SUBSTR(calledpartynumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,9,1))) = HEX(LOWER(SUBSTR(calledpartynumber,9,1))) THEN SUBSTR(calledpartynumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,10,1))) = HEX(LOWER(SUBSTR(calledpartynumber,10,1))) THEN SUBSTR(calledpartynumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,11,1))) = HEX(LOWER(SUBSTR(calledpartynumber,11,1))) THEN SUBSTR(calledpartynumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,12,1))) = HEX(LOWER(SUBSTR(calledpartynumber,12,1))) THEN SUBSTR(calledpartynumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,13,1))) = HEX(LOWER(SUBSTR(calledpartynumber,13,1))) THEN SUBSTR(calledpartynumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,14,1))) = HEX(LOWER(SUBSTR(calledpartynumber,14,1))) THEN SUBSTR(calledpartynumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,15,1))) = HEX(LOWER(SUBSTR(calledpartynumber,15,1))) THEN SUBSTR(calledpartynumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,16,1))) = HEX(LOWER(SUBSTR(calledpartynumber,16,1))) THEN SUBSTR(calledpartynumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,17,1))) = HEX(LOWER(SUBSTR(calledpartynumber,17,1))) THEN SUBSTR(calledpartynumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,18,1))) = HEX(LOWER(SUBSTR(calledpartynumber,18,1))) THEN SUBSTR(calledpartynumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,19,1))) = HEX(LOWER(SUBSTR(calledpartynumber,19,1))) THEN SUBSTR(calledpartynumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,20,1))) = HEX(LOWER(SUBSTR(calledpartynumber,20,1))) THEN SUBSTR(calledpartynumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,21,1))) = HEX(LOWER(SUBSTR(calledpartynumber,21,1))) THEN SUBSTR(calledpartynumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,22,1))) = HEX(LOWER(SUBSTR(calledpartynumber,22,1))) THEN SUBSTR(calledpartynumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,23,1))) = HEX(LOWER(SUBSTR(calledpartynumber,23,1))) THEN SUBSTR(calledpartynumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,24,1))) = HEX(LOWER(SUBSTR(calledpartynumber,24,1))) THEN SUBSTR(calledpartynumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,25,1))) = HEX(LOWER(SUBSTR(calledpartynumber,25,1))) THEN SUBSTR(calledpartynumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,26,1))) = HEX(LOWER(SUBSTR(calledpartynumber,26,1))) THEN SUBSTR(calledpartynumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,27,1))) = HEX(LOWER(SUBSTR(calledpartynumber,27,1))) THEN SUBSTR(calledpartynumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,28,1))) = HEX(LOWER(SUBSTR(calledpartynumber,28,1))) THEN SUBSTR(calledpartynumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,29,1))) = HEX(LOWER(SUBSTR(calledpartynumber,29,1))) THEN SUBSTR(calledpartynumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,30,1))) = HEX(LOWER(SUBSTR(calledpartynumber,30,1))) THEN SUBSTR(calledpartynumber,30,1) ELSE ''  END )),' '),'^[01]*','') AS callednumber6_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,3,1))) = HEX(LOWER(SUBSTR(calledpartynumber,3,1))) THEN SUBSTR(calledpartynumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,4,1))) = HEX(LOWER(SUBSTR(calledpartynumber,4,1))) THEN SUBSTR(calledpartynumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,5,1))) = HEX(LOWER(SUBSTR(calledpartynumber,5,1))) THEN SUBSTR(calledpartynumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,6,1))) = HEX(LOWER(SUBSTR(calledpartynumber,6,1))) THEN SUBSTR(calledpartynumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,7,1))) = HEX(LOWER(SUBSTR(calledpartynumber,7,1))) THEN SUBSTR(calledpartynumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,8,1))) = HEX(LOWER(SUBSTR(calledpartynumber,8,1))) THEN SUBSTR(calledpartynumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,9,1))) = HEX(LOWER(SUBSTR(calledpartynumber,9,1))) THEN SUBSTR(calledpartynumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,10,1))) = HEX(LOWER(SUBSTR(calledpartynumber,10,1))) THEN SUBSTR(calledpartynumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,11,1))) = HEX(LOWER(SUBSTR(calledpartynumber,11,1))) THEN SUBSTR(calledpartynumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,12,1))) = HEX(LOWER(SUBSTR(calledpartynumber,12,1))) THEN SUBSTR(calledpartynumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,13,1))) = HEX(LOWER(SUBSTR(calledpartynumber,13,1))) THEN SUBSTR(calledpartynumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,14,1))) = HEX(LOWER(SUBSTR(calledpartynumber,14,1))) THEN SUBSTR(calledpartynumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,15,1))) = HEX(LOWER(SUBSTR(calledpartynumber,15,1))) THEN SUBSTR(calledpartynumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,16,1))) = HEX(LOWER(SUBSTR(calledpartynumber,16,1))) THEN SUBSTR(calledpartynumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,17,1))) = HEX(LOWER(SUBSTR(calledpartynumber,17,1))) THEN SUBSTR(calledpartynumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,18,1))) = HEX(LOWER(SUBSTR(calledpartynumber,18,1))) THEN SUBSTR(calledpartynumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,19,1))) = HEX(LOWER(SUBSTR(calledpartynumber,19,1))) THEN SUBSTR(calledpartynumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,20,1))) = HEX(LOWER(SUBSTR(calledpartynumber,20,1))) THEN SUBSTR(calledpartynumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,21,1))) = HEX(LOWER(SUBSTR(calledpartynumber,21,1))) THEN SUBSTR(calledpartynumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,22,1))) = HEX(LOWER(SUBSTR(calledpartynumber,22,1))) THEN SUBSTR(calledpartynumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,23,1))) = HEX(LOWER(SUBSTR(calledpartynumber,23,1))) THEN SUBSTR(calledpartynumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,24,1))) = HEX(LOWER(SUBSTR(calledpartynumber,24,1))) THEN SUBSTR(calledpartynumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,25,1))) = HEX(LOWER(SUBSTR(calledpartynumber,25,1))) THEN SUBSTR(calledpartynumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,26,1))) = HEX(LOWER(SUBSTR(calledpartynumber,26,1))) THEN SUBSTR(calledpartynumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,27,1))) = HEX(LOWER(SUBSTR(calledpartynumber,27,1))) THEN SUBSTR(calledpartynumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,28,1))) = HEX(LOWER(SUBSTR(calledpartynumber,28,1))) THEN SUBSTR(calledpartynumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,29,1))) = HEX(LOWER(SUBSTR(calledpartynumber,29,1))) THEN SUBSTR(calledpartynumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(calledpartynumber,30,1))) = HEX(LOWER(SUBSTR(calledpartynumber,30,1))) THEN SUBSTR(calledpartynumber,30,1) ELSE ''  END )),' '),'^[01]*','') AS callednumber3_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,3,1))) = HEX(LOWER(SUBSTR(originalcallednumber,3,1))) THEN SUBSTR(originalcallednumber,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,4,1))) = HEX(LOWER(SUBSTR(originalcallednumber,4,1))) THEN SUBSTR(originalcallednumber,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,5,1))) = HEX(LOWER(SUBSTR(originalcallednumber,5,1))) THEN SUBSTR(originalcallednumber,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,6,1))) = HEX(LOWER(SUBSTR(originalcallednumber,6,1))) THEN SUBSTR(originalcallednumber,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,7,1))) = HEX(LOWER(SUBSTR(originalcallednumber,7,1))) THEN SUBSTR(originalcallednumber,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,8,1))) = HEX(LOWER(SUBSTR(originalcallednumber,8,1))) THEN SUBSTR(originalcallednumber,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,9,1))) = HEX(LOWER(SUBSTR(originalcallednumber,9,1))) THEN SUBSTR(originalcallednumber,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,10,1))) = HEX(LOWER(SUBSTR(originalcallednumber,10,1))) THEN SUBSTR(originalcallednumber,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,11,1))) = HEX(LOWER(SUBSTR(originalcallednumber,11,1))) THEN SUBSTR(originalcallednumber,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,12,1))) = HEX(LOWER(SUBSTR(originalcallednumber,12,1))) THEN SUBSTR(originalcallednumber,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,13,1))) = HEX(LOWER(SUBSTR(originalcallednumber,13,1))) THEN SUBSTR(originalcallednumber,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,14,1))) = HEX(LOWER(SUBSTR(originalcallednumber,14,1))) THEN SUBSTR(originalcallednumber,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,15,1))) = HEX(LOWER(SUBSTR(originalcallednumber,15,1))) THEN SUBSTR(originalcallednumber,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,16,1))) = HEX(LOWER(SUBSTR(originalcallednumber,16,1))) THEN SUBSTR(originalcallednumber,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,17,1))) = HEX(LOWER(SUBSTR(originalcallednumber,17,1))) THEN SUBSTR(originalcallednumber,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,18,1))) = HEX(LOWER(SUBSTR(originalcallednumber,18,1))) THEN SUBSTR(originalcallednumber,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,19,1))) = HEX(LOWER(SUBSTR(originalcallednumber,19,1))) THEN SUBSTR(originalcallednumber,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,20,1))) = HEX(LOWER(SUBSTR(originalcallednumber,20,1))) THEN SUBSTR(originalcallednumber,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,21,1))) = HEX(LOWER(SUBSTR(originalcallednumber,21,1))) THEN SUBSTR(originalcallednumber,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,22,1))) = HEX(LOWER(SUBSTR(originalcallednumber,22,1))) THEN SUBSTR(originalcallednumber,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,23,1))) = HEX(LOWER(SUBSTR(originalcallednumber,23,1))) THEN SUBSTR(originalcallednumber,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,24,1))) = HEX(LOWER(SUBSTR(originalcallednumber,24,1))) THEN SUBSTR(originalcallednumber,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,25,1))) = HEX(LOWER(SUBSTR(originalcallednumber,25,1))) THEN SUBSTR(originalcallednumber,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,26,1))) = HEX(LOWER(SUBSTR(originalcallednumber,26,1))) THEN SUBSTR(originalcallednumber,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,27,1))) = HEX(LOWER(SUBSTR(originalcallednumber,27,1))) THEN SUBSTR(originalcallednumber,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,28,1))) = HEX(LOWER(SUBSTR(originalcallednumber,28,1))) THEN SUBSTR(originalcallednumber,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,29,1))) = HEX(LOWER(SUBSTR(originalcallednumber,29,1))) THEN SUBSTR(originalcallednumber,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(originalcallednumber,30,1))) = HEX(LOWER(SUBSTR(originalcallednumber,30,1))) THEN SUBSTR(originalcallednumber,30,1) ELSE ''  END  )),' '),'^[01]*','') AS originalcalled3_t,

REGEXP_REPLACE(COALESCE((
CONCAT(  
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,3,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,3,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,4,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,4,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,5,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,5,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,6,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,6,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END  )),' '),'^[01]*','') AS REDIRECTINGNUMBER3_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END  )),' '),'^[01]*','') AS REDIRECTINGNUMBER7_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END )),' '),'^[01]*','') AS REDIRECTINGNUMBER8_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) = HEX(LOWER(SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1))) THEN SUBSTR(CASE WHEN RECORD_TYPE IN ('mSOriginating') THEN translatedNumber ELSE redirectingnumber END,30,1) ELSE ''  END )),' '),'^[01]*','')  AS REDIRECTINGNUMBER11_t
FROM cdrdm.gsm1205) a ${hiveconf:WHERE_clause} AND RECORD_TYPE IN ('mSOriginating','roamingCallForwarding','callForwarding','mSTerminating','mSOriginatingSMSinMSC','mSTerminatingSMSinMSC');

CREATE TABLE IF NOT EXISTS ext_cdrdm.gsm1205_temp2 AS
SELECT
--Data Mart
(CASE
WHEN RECORD_TYPE = 'transit' THEN (CASE WHEN cleansedcharged = '' OR cleansedcharged = 0 THEN (CASE WHEN CleansedCalling = '' OR CleansedCalling = 0 THEN (CASE WHEN cleansedcalled = '' OR cleansedcalled = 0 THEN -recordsequencenumber ELSE CleansedCalled END) 
                  ELSE cleansedcalling END) ELSE cleansedCharged END)
WHEN RECORD_TYPE = 'mSOriginating' THEN CASE  WHEN cleansedCharged = '' OR cleansedCharged = 0 THEN CASE WHEN CleansedCalling = '' OR CleansedCalling = 0 THEN recordsequencenumber ELSE CleansedCalling END ELSE cleansedCharged END
WHEN RECORD_TYPE = 'roamingCallForwarding' THEN CASE  WHEN cleansedCharged = '' OR cleansedCharged = 0 THEN CASE WHEN CleansedCalled = '' OR CleansedCalled = 0 THEN -recordsequencenumber ELSE CleansedCalled END ELSE cleansedCharged  END
WHEN RECORD_TYPE = 'callForwarding' THEN CASE  WHEN CleansedRedirecting = '' OR CleansedRedirecting = 0 THEN CASE WHEN cleansedCharged = '' OR cleansedCharged = 0 THEN -recordsequencenumber ELSE cleansedCharged END ELSE COALESCE(CleansedRedirecting,0) END
WHEN RECORD_TYPE = 'mSOriginatingSMSinMSC' THEN CASE  WHEN CleansedCalling = '' OR CleansedCalling = 0 THEN  -recordsequencenumber ELSE CleansedCalling END
WHEN RECORD_TYPE IN ('mSTerminating','mSTerminatingSMSinMSC') THEN CASE
                        WHEN CleansedCalled = 0 OR CleansedCalled = '' THEN CASE WHEN CleansedRedirecting IS NULL OR CleansedRedirecting = '' OR CleansedRedirecting = 0 THEN  
                        CASE WHEN CleansedOriginal = 0 OR CleansedOriginal = '' THEN -recordsequencenumber ELSE CleansedOriginal END ELSE COALESCE(CleansedRedirecting,0) END
                        ELSE CleansedCalled END
                        ELSE -recordsequencenumber END) AS Subscriber_No,
(CASE WHEN CleansedCalling = 0 OR CleansedCalling = ''
THEN -1 ELSE CleansedCalling END) AS cleansed_CALLING_NUMBER,                     
(CASE WHEN (RECORD_TYPE IN ('mSOriginating')
           AND CleansedRedirecting <> cleansedCalled)
           AND LENGTH(CAST(CleansedCalled AS STRING)) <> LENGTH(CAST(CleansedRedirecting AS STRING))
           AND LENGTH(CAST(CleansedCalled AS STRING)) > 6
      THEN
           (CASE 
              WHEN (CleansedRedirecting IS NULL OR CleansedRedirecting = 0 OR CleansedRedirecting = '')
              THEN -1
              ELSE CleansedRedirecting 
            END)
      ELSE
           (CASE WHEN CleansedCalled = 0 OR CleansedCalled = ''
                 THEN (CASE WHEN ( CleansedRedirecting IS NULL OR CleansedRedirecting = 0 OR CleansedRedirecting = '')
                            THEN -1 
                            ELSE CleansedRedirecting 
                       END)
                 ELSE CleansedCalled 
            END)
      END) AS Cleansed_CALLED_NUMBER,     
(CASE 
     WHEN (RECORD_TYPE IN ('mSOriginating') AND (cleansedOriginal = 0 OR cleansedOriginal = ''))
     THEN
          CASE WHEN CleansedCalled = 0 OR CleansedCalled = '' THEN -1
               ELSE CleansedCalled
          END
     ELSE
          CASE WHEN CleansedOriginal = 0 OR CleansedOriginal = '' THEN -1
               ELSE CleansedOriginal
          END
 END) AS Cleansed_ORIGINAL_NUMBER,
CASE WHEN CleansedRedirecting IS NULL OR CleansedRedirecting = ''
     THEN -1 ELSE COALESCE(CleansedRedirecting,0) END AS Cleansed_REDIRECTING_NUMBER,
(CASE WHEN cleansedcharged = 0 OR cleansedcharged = '' THEN -1
      ELSE cleansedcharged END) AS Cleansed_CHARGED_NUMBER , 
REGEXP_REPLACE(callingpartynumber,'^F*|F$*','') AS CALLING_PARTY_NUMBER,
REGEXP_REPLACE(calledpartynumber,'^F*|F$*','') AS CALLED_PARTY_NUMBER,
REGEXP_REPLACE(originalcallednumber,'^F*|F$*','') AS ORIGINAL_CALLED_NUMBER,
REGEXP_REPLACE(redirectingnumber,'^F*|F$*','') AS REDIRECTING_NUMBER,
REGEXP_REPLACE(chargenumber,'^F*|F$*','') AS CHARGED_PARTY_NUMBER,
(CASE WHEN (LENGTH(MobileStationRoamingNumber) = 0 OR MobileStationRoamingNumber IS NULL) THEN 0 ELSE CAST(REGEXP_REPLACE(TRIM(MobileStationRoamingNumber),'^F*|F$*','') AS BIGINT) END) AS MOBILE_STATION_ROAMING_NBR,
Recordsequencenumber,
(CASE WHEN (LENGTH( TRIM (SUBSTR(IMSI,1,15))) = 0 OR SUBSTR(IMSI,1,15) IS NULL)
       THEN -1 ELSE CAST(SUBSTR(IMSI,1,15) AS BIGINT) END) AS IMSI,
(CASE WHEN (LENGTH( TRIM(SUBSTR(IMEI,1,15))) = 0 OR SUBSTR(IMEI,1,15) IS NULL)
       THEN -1 ELSE CAST(SUBSTR(IMEI,1,15) AS BIGINT) END) AS IMEI,
CAST(CONCAT(date_for_start_of_charge,' ',SUBSTR (time_for_start_of_charge,1,8)) AS STRING) AS Call_Timestamp,
CASE WHEN chargeableduration = '' THEN NULL 
     ELSE (CAST(SUBSTR(chargeableduration,1,2) AS INT)*3600
       +CAST(SUBSTR(chargeableduration,4,2) AS INT)*60
       +CAST(SUBSTR(chargeableduration,7,2) AS INT)) 
END AS CHARGEABLE_DURATION,
(CASE WHEN timefortcseizure = '' THEN -1 
      ELSE (CAST(SUBSTR(timefortcseizure,1,2) AS INT)*3600
            +CAST(SUBSTR(timefortcseizure,3,2) AS INT)*60
            +CAST(SUBSTR(timefortcseizure,5,2) AS INT)) 
      END) AS Register_Seizure_Charging_St,
--Network fields:            
TRIM(incomingroute) AS incomingroute,
TRIM(outgoingroute) AS outgoingroute,
TRIM(CellIDForFirstCell) AS First_Cell_Id,
TRIM(CellIDForLastCell) AS Last_Cell_Id,
exchangeidentity,      
switchIdentity,
--Error fields:
CAST(partialoutputrecnum AS SMALLINT) Partial_Output_Num,
COALESCE(FaultCode,'') AS Fault_Code,
CAST(EOSInfo AS BIGINT) AS EOS_INFO,
disconnectingparty,                  
internalCauseAndLoc,
--Revenue Assurance fields:
TRIM(trafficactivitycode) AS Traffic_Account_Code,
CAST(ChargedParty AS SMALLINT) AS Charged_Party,
regionDependentChargingOrigin,
multimediacall,
teleservicecode,
tariffclass,
firstlocationinformationextension,
subscriptionType,
SRVCCIndicator,
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
FROM ext_cdrdm.gsm1205_temp1 CDR;
--FROM ext_cdrdm.gsm1205_temp1 CDR WHERE CleansedCalling <= 9223372036854775807;

INSERT INTO TABLE cdrdm.FACT_GSM_CDR_TEMP PARTITION (call_timestamp_date)
SELECT 
cdr.subscriber_no,
cdr.recordsequencenumber,
CASE WHEN cdr.record_type = 'mSOriginating' THEN 2
WHEN cdr.record_type = 'roamingCallForwarding' THEN 3
WHEN cdr.record_type = 'callForwarding' THEN 4
WHEN cdr.record_type = 'mSTerminating' THEN 5
WHEN cdr.record_type = 'mSOriginatingSMSinMSC' THEN 6
WHEN cdr.record_type = 'mSTerminatingSMSinMSC' THEN 8 END as call_type,
cdr.imsi,
cdr.imei,
cdr.exchangeidentity,
cdr.switchIdentity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.register_seizure_charging_st AS reg_seizure_charging_start,
cdr.mobile_station_roaming_nbr AS mobile_station_roaming_number,
cdr.redirecting_number,
CASE WHEN COALESCE(cleansed_redirecting_number,0) = 0 THEN - 1 ELSE COALESCE(cleansed_redirecting_number,0) END AS cleansed_redirecting_number,
cdr.fault_code,
CASE WHEN (cdr.eos_info = 0 AND cdr.record_type = 'mSOriginatingSMSinMSC') THEN NULL ELSE cdr.eos_info END AS eos_info,
CASE WHEN TRIM(incomingroute) = '' THEN NULL ELSE incomingroute END AS incoming_route_id, 
CASE WHEN TRIM(outgoingroute) = '' THEN NULL ELSE outgoingroute END AS outgoing_route_id,
cdr.first_cell_id,
cdr.last_cell_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internalcauseandloc,
CASE WHEN TRIM(cdr.traffic_account_code) = '' THEN NULL ELSE cdr.traffic_account_code END AS traffic_activity_code,
cdr.disconnectingparty,
cdr.partial_output_num,
cdr.regiondependentchargingorigin,
CASE cdr.record_type WHEN 'mSOriginatingSMSinMSC' THEN 'N/A' ELSE 'XXXX' END AS ocn,
cdr.multimediacall,
cdr.teleservicecode,
CASE WHEN TRIM(cdr.tariffclass) = 0 THEN NULL ELSE cdr.tariffclass END AS tariffclass,
cdr.firstlocationinformationextension,
cdr.subscriptionType,
cdr.SRVCCIndicator,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
from_unixtime(unix_timestamp()),
substr(cdr.call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.gsm1205_temp2 CDR
WHERE CDR.record_type IN ('mSOriginating','roamingCallForwarding','callForwarding','mSOriginatingSMSinMSC')
AND CDR.Cleansed_CALLED_NUMBER < 1000000000
 
UNION ALL

SELECT 
cdr.subscriber_no,
cdr.recordsequencenumber,
CASE WHEN cdr.record_type = 'mSOriginating' THEN 2
WHEN cdr.record_type = 'roamingCallForwarding' THEN 3
WHEN cdr.record_type = 'callForwarding' THEN 4
WHEN cdr.record_type = 'mSTerminating' THEN 5
WHEN cdr.record_type = 'mSOriginatingSMSinMSC' THEN 6
WHEN cdr.record_type = 'mSTerminatingSMSinMSC' THEN 8 END as call_type,
cdr.imsi,
cdr.imei,
cdr.exchangeidentity,
cdr.switchIdentity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.register_seizure_charging_st AS reg_seizure_charging_start,
cdr.mobile_station_roaming_nbr AS mobile_station_roaming_number,
cdr.redirecting_number,
CASE WHEN COALESCE(cleansed_redirecting_number,0) = 0 THEN - 1 ELSE COALESCE(cleansed_redirecting_number,0) END AS cleansed_redirecting_number,
cdr.fault_code,
CASE WHEN (cdr.eos_info = 0  AND CDR.record_type = 'mSTerminatingSMSinMSC') THEN NULL ELSE cdr.eos_info END AS eos_info,
CASE WHEN TRIM(cdr.incomingroute) = '' THEN NULL ELSE cdr.incomingroute END AS incoming_route_id,
CASE WHEN TRIM(cdr.outgoingroute) = '' THEN NULL ELSE cdr.outgoingroute END AS outgoing_route_id,
cdr.first_cell_id,
cdr.last_cell_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internalcauseandloc,
CASE WHEN TRIM(cdr.traffic_account_code) = '' THEN NULL ELSE cdr.traffic_account_code END AS traffic_activity_code,
cdr.disconnectingparty,
cdr.partial_output_num,
cdr.regiondependentchargingorigin,
CASE cdr.record_type WHEN 'mSTerminatingSMSinMSC' THEN 'N/A' ELSE 'XXXX' END AS ocn,
cdr.multimediacall,
cdr.teleservicecode,
CASE WHEN TRIM(cdr.tariffclass) = 0 THEN NULL ELSE cdr.tariffclass END AS tariffclass,
cdr.firstlocationinformationextension, 
cdr.subscriptionType,
cdr.SRVCCIndicator,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
from_unixtime(unix_timestamp()),
substr(cdr.call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.gsm1205_temp2 CDR
WHERE CDR.record_type IN ('mSTerminating','mSTerminatingSMSinMSC')
AND CDR.Cleansed_CALLING_NUMBER < 1000000000
 
UNION ALL 

SELECT
cdr.subscriber_no,
cdr.recordsequencenumber,
CASE WHEN cdr.record_type = 'mSOriginating' THEN 2
WHEN cdr.record_type = 'roamingCallForwarding' THEN 3
WHEN cdr.record_type = 'callForwarding' THEN 4
WHEN cdr.record_type = 'mSTerminating' THEN 5
WHEN cdr.record_type = 'mSOriginatingSMSinMSC' THEN 6
WHEN cdr.record_type = 'mSTerminatingSMSinMSC' THEN 8 END as call_type,
cdr.imsi,
cdr.imei,
cdr.exchangeidentity,
cdr.switchIdentity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.register_seizure_charging_st AS reg_seizure_charging_start,
cdr.mobile_station_roaming_nbr AS mobile_station_roaming_number,
cdr.redirecting_number,
CASE WHEN COALESCE(cleansed_redirecting_number,0) = 0 THEN - 1 ELSE COALESCE(cleansed_redirecting_number,0) END AS cleansed_redirecting_number,
cdr.fault_code,
CASE WHEN (cdr.eos_info = 0 AND cdr.record_type = 'mSOriginatingSMSinMSC') THEN NULL ELSE cdr.eos_info END AS eos_info,
CASE WHEN TRIM(incomingroute) = '' THEN NULL ELSE incomingroute  END AS incoming_route_id,
CASE WHEN TRIM(outgoingroute) = '' THEN NULL ELSE outgoingroute  END AS outgoing_route_id,
cdr.first_cell_id,
cdr.last_cell_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internalcauseandloc,
CASE WHEN TRIM(cdr.traffic_account_code) = '' THEN NULL ELSE cdr.traffic_account_code END AS traffic_activity_code, 
cdr.disconnectingparty,
cdr.partial_output_num,
cdr.regiondependentchargingorigin,
COALESCE(int_source.ocn,'XXXX') AS ocn, 
cdr.multimediacall,
cdr.teleservicecode,
CASE WHEN TRIM(cdr.tariffclass) = 0 THEN NULL ELSE cdr.tariffclass END AS tariffclass,
cdr.firstlocationinformationextension,
cdr.subscriptionType,
cdr.SRVCCIndicator,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
from_unixtime(unix_timestamp()),
substr(cdr.call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.gsm1205_temp2 CDR
LEFT OUTER JOIN cdrdm.dim_interaction_source int_source
ON (SUBSTR(TRIM(CAST (CDR.cleansed_called_number AS STRING)),1,3) = int_source.NPA
AND SUBSTR(TRIM(CAST (CDR.cleansed_called_number AS STRING)),4,3) = int_source.NXX)
WHERE CDR.record_type IN ('mSOriginating','roamingCallForwarding','callForwarding','mSOriginatingSMSinMSC')
AND CDR.Cleansed_CALLED_NUMBER >= 1000000000

UNION ALL

SELECT
cdr.subscriber_no,
cdr.recordsequencenumber,
CASE WHEN cdr.record_type = 'mSOriginating' THEN 2
WHEN cdr.record_type = 'roamingCallForwarding' THEN 3
WHEN cdr.record_type = 'callForwarding' THEN 4
WHEN cdr.record_type = 'mSTerminating' THEN 5
WHEN cdr.record_type = 'mSOriginatingSMSinMSC' THEN 6
WHEN cdr.record_type = 'mSTerminatingSMSinMSC' THEN 8 END as call_type,
cdr.imsi,
cdr.imei,
cdr.exchangeidentity,
cdr.switchIdentity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.register_seizure_charging_st AS reg_seizure_charging_start,
cdr.mobile_station_roaming_nbr AS mobile_station_roaming_number,
cdr.redirecting_number,
CASE WHEN COALESCE(cleansed_redirecting_number,0) = 0 THEN - 1 ELSE COALESCE(cleansed_redirecting_number,0) END AS cleansed_redirecting_number,
cdr.fault_code,
CASE WHEN (cdr.eos_info = 0 AND CDR.record_type = 'mSTerminatingSMSinMSC') THEN NULL ELSE cdr.eos_info END AS eos_info, 
CASE WHEN TRIM(incomingroute) = '' THEN NULL ELSE incomingroute END AS incoming_route_id,
CASE WHEN TRIM(outgoingroute) = '' THEN NULL ELSE outgoingroute END AS outgoing_route_id,
cdr.first_cell_id,
cdr.last_cell_id,
cdr.charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internalcauseandloc,
CASE WHEN TRIM(cdr.traffic_account_code) = '' THEN NULL ELSE cdr.traffic_account_code END AS traffic_activity_code,
cdr.disconnectingparty,
cdr.partial_output_num,
cdr.regiondependentchargingorigin,
COALESCE(int_source.ocn,'XXXX') AS ocn,
cdr.multimediacall,
cdr.teleservicecode,
CASE WHEN TRIM(cdr.tariffclass) = 0 THEN NULL ELSE cdr.tariffclass END AS tariffclass,
cdr.firstlocationinformationextension,
cdr.subscriptionType,
cdr.SRVCCIndicator,
cdr.file_name,
cdr.record_start,
cdr.record_end,
cdr.record_type,
cdr.family_name,
cdr.version_id,
cdr.file_time,
cdr.file_id,
cdr.switch_name,
cdr.num_records,
from_unixtime(unix_timestamp()),
substr(cdr.call_timestamp,1,10) as call_timestamp_date
FROM ext_cdrdm.gsm1205_temp2 CDR 
LEFT OUTER JOIN cdrdm.dim_interaction_source int_source 
ON (SUBSTR(TRIM(CAST (CDR.cleansed_calling_number AS STRING)),1,3) = int_source.NPA 
AND SUBSTR(TRIM(CAST (CDR.cleansed_calling_number AS STRING)),4,3) = int_source.NXX)
WHERE CDR.record_type IN ('mSTerminating','mSTerminatingSMSinMSC')
AND CDR.Cleansed_CALLING_NUMBER >= 1000000000;

DROP TABLE IF EXISTS ext_cdrdm.gsm1205_temp1;
DROP TABLE IF EXISTS ext_cdrdm.gsm1205_temp2;