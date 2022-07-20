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

DROP TABLE IF EXISTS ext_cdrdm.gsm1205_transit_temp1;
DROP TABLE IF EXISTS ext_cdrdm.gsm1205_transit_temp2;

CREATE TABLE IF NOT EXISTS ext_cdrdm.gsm1205_transit_temp1 AS 
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
ELSE CASE WHEN SUBSTR(REDIRECTINGNUMBER,1,4) IN ('14BB') THEN 
CASE WHEN LENGTH(REDIRECTINGNUMBER7_t) < 10 THEN REDIRECTINGNUMBER7_t END 
WHEN SUBSTR(REDIRECTINGNUMBER,1,4) IN ('14B3') THEN 
CASE WHEN LENGTH(REDIRECTINGNUMBER8_t) < 10 THEN REDIRECTINGNUMBER8_t END        
WHEN SUBSTR(REDIRECTINGNUMBER,1,4) IN ('14AA') THEN 
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
redirectingnumber,
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
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,3,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,3,1))) THEN SUBSTR( REDIRECTINGNUMBER,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,4,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,4,1))) THEN SUBSTR( REDIRECTINGNUMBER,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,5,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,5,1))) THEN SUBSTR( REDIRECTINGNUMBER,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,6,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,6,1))) THEN SUBSTR( REDIRECTINGNUMBER,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,7,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,7,1))) THEN SUBSTR( REDIRECTINGNUMBER,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,8,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,8,1))) THEN SUBSTR( REDIRECTINGNUMBER,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,9,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,9,1))) THEN SUBSTR( REDIRECTINGNUMBER,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,10,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,10,1))) THEN SUBSTR( REDIRECTINGNUMBER,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END )
AS REDIRECTINGNUMBER3,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,7,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,7,1))) THEN SUBSTR( REDIRECTINGNUMBER,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,8,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,8,1))) THEN SUBSTR( REDIRECTINGNUMBER,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,9,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,9,1))) THEN SUBSTR( REDIRECTINGNUMBER,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,10,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,10,1))) THEN SUBSTR( REDIRECTINGNUMBER,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END )
AS REDIRECTINGNUMBER7,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,8,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,8,1))) THEN SUBSTR( REDIRECTINGNUMBER,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,9,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,9,1))) THEN SUBSTR( REDIRECTINGNUMBER,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,10,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,10,1))) THEN SUBSTR( REDIRECTINGNUMBER,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END )
AS REDIRECTINGNUMBER8,

CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END )
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
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,3,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,3,1))) THEN SUBSTR( REDIRECTINGNUMBER,3,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,4,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,4,1))) THEN SUBSTR( REDIRECTINGNUMBER,4,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,5,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,5,1))) THEN SUBSTR( REDIRECTINGNUMBER,5,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,6,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,6,1))) THEN SUBSTR( REDIRECTINGNUMBER,6,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,7,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,7,1))) THEN SUBSTR( REDIRECTINGNUMBER,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,8,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,8,1))) THEN SUBSTR( REDIRECTINGNUMBER,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,9,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,9,1))) THEN SUBSTR( REDIRECTINGNUMBER,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,10,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,10,1))) THEN SUBSTR( REDIRECTINGNUMBER,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END  )),' '),'^[01]*','') AS REDIRECTINGNUMBER3_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,7,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,7,1))) THEN SUBSTR( REDIRECTINGNUMBER,7,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,8,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,8,1))) THEN SUBSTR( REDIRECTINGNUMBER,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,9,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,9,1))) THEN SUBSTR( REDIRECTINGNUMBER,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,10,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,10,1))) THEN SUBSTR( REDIRECTINGNUMBER,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END  )),' '),'^[01]*','') AS REDIRECTINGNUMBER7_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,8,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,8,1))) THEN SUBSTR( REDIRECTINGNUMBER,8,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,9,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,9,1))) THEN SUBSTR( REDIRECTINGNUMBER,9,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,10,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,10,1))) THEN SUBSTR( REDIRECTINGNUMBER,10,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END )),' '),'^[01]*','') AS REDIRECTINGNUMBER8_t,

REGEXP_REPLACE(COALESCE((
CONCAT(
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,11,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,11,1))) THEN SUBSTR( REDIRECTINGNUMBER,11,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,12,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,12,1))) THEN SUBSTR( REDIRECTINGNUMBER,12,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,13,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,13,1))) THEN SUBSTR( REDIRECTINGNUMBER,13,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,14,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,14,1))) THEN SUBSTR( REDIRECTINGNUMBER,14,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,15,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,15,1))) THEN SUBSTR( REDIRECTINGNUMBER,15,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,16,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,16,1))) THEN SUBSTR( REDIRECTINGNUMBER,16,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,17,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,17,1))) THEN SUBSTR( REDIRECTINGNUMBER,17,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,18,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,18,1))) THEN SUBSTR( REDIRECTINGNUMBER,18,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,19,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,19,1))) THEN SUBSTR( REDIRECTINGNUMBER,19,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,20,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,20,1))) THEN SUBSTR( REDIRECTINGNUMBER,20,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,21,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,21,1))) THEN SUBSTR( REDIRECTINGNUMBER,21,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,22,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,22,1))) THEN SUBSTR( REDIRECTINGNUMBER,22,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,23,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,23,1))) THEN SUBSTR( REDIRECTINGNUMBER,23,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,24,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,24,1))) THEN SUBSTR( REDIRECTINGNUMBER,24,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,25,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,25,1))) THEN SUBSTR( REDIRECTINGNUMBER,25,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,26,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,26,1))) THEN SUBSTR( REDIRECTINGNUMBER,26,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,27,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,27,1))) THEN SUBSTR( REDIRECTINGNUMBER,27,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,28,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,28,1))) THEN SUBSTR( REDIRECTINGNUMBER,28,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,29,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,29,1))) THEN SUBSTR( REDIRECTINGNUMBER,29,1) ELSE ''  END ,
CASE  WHEN HEX(UPPER(SUBSTR(REDIRECTINGNUMBER,30,1))) = HEX(LOWER(SUBSTR(REDIRECTINGNUMBER,30,1))) THEN SUBSTR( REDIRECTINGNUMBER,30,1) ELSE ''  END )),' '),'^[01]*','')  AS REDIRECTINGNUMBER11_t
FROM cdrdm.gsm1205) a ${hiveconf:WHERE_clause} AND RECORD_TYPE = 'transit';


create table ext_cdrdm.gsm1205_transit_temp2 as                                 
SELECT
Recordsequencenumber,
(CASE WHEN (LENGTH(TRIM(SUBSTR(IMSI,1,15))) = 0  OR (SUBSTR(IMSI,1,15) IS NULL)) THEN -1 ELSE CAST(SUBSTR(IMSI,1,15) AS BIGINT) END) AS IMSI,
exchangeidentity,
switchIdentity,
CAST(CONCAT(date_for_start_of_charge,' ',SUBSTR (time_for_start_of_charge,1,8)) AS STRING) AS Call_Timestamp,
(CASE WHEN chargeableduration = '' THEN NULL ELSE (SUBSTR (chargeableduration,1,2) * 3600 + SUBSTR (chargeableduration,4,2) * 60 + SUBSTR (chargeableduration,7,2)) END) AS CHARGEABLE_DURATION,  
REGEXP_REPLACE(callingpartynumber,'^F*|F$*','') AS CALLING_PARTY_NUMBER,
CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END AS Cleansed_CALLING_NUMBER,
REGEXP_REPLACE(calledpartynumber,'^F*|F$*','') AS CALLED_PARTY_NUMBER,
(CASE 
  WHEN (RECORD_TYPE IN ('callForwarding') 
         AND CleansedRedirecting <> cleansedCalled 
         AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING)))
         AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) 
  THEN
      (CASE  
        WHEN CleansedRedirecting = 0 THEN - 1 
        ELSE CleansedRedirecting END)
  ELSE
      (CASE  
        WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) 
        ELSE CleansedCalled END)
END) AS Cleansed_CALLED_NUMBER,
REGEXP_REPLACE(originalcallednumber,'^F*|F$*','') AS ORIGINAL_CALLED_NUMBER,
(CASE 
 WHEN (RECORD_TYPE IN ('callForwarding') AND cleansedOriginal = 0)
 THEN       
   (CASE  
       WHEN CleansedCalled = 0 THEN - 1
       ELSE CleansedCalled  
    END)
 ELSE
    (CASE  
       WHEN CleansedOriginal = 0 THEN - 1
       ELSE CleansedOriginal 
     END)
END) AS Cleansed_ORIGINAL_NUMBER,
(CASE WHEN timefortcseizure = '' THEN -1 
      ELSE (CAST(SUBSTR(timefortcseizure,1,2) AS INT)*3600
            +CAST(SUBSTR(timefortcseizure,3,2) AS INT)*60
            +CAST(SUBSTR(timefortcseizure,5,2) AS INT)) 
      END) AS reg_seizure_charging_start,
REGEXP_REPLACE(RedirectingNumber,'^F*|F$*','') AS REDIRECTING_NUMBER,
CASE WHEN COALESCE(CleansedRedirecting,0) = 0 THEN - 1 ELSE COALESCE(CleansedRedirecting,0) END AS Cleansed_REDIRECTING_NUMBER,
COALESCE(FaultCode,'') AS Fault_Code,
(CASE WHEN TRIM(trafficactivitycode) = '' THEN NULL ELSE EOSInfo END) AS EOSInfo,
(CASE WHEN TRIM(incomingroute) = '' THEN NULL ELSE TRIM(incomingroute) END) AS incomingroute,
(CASE WHEN TRIM(outgoingroute) = '' THEN NULL ELSE TRIM(outgoingroute) END) AS outgoingroute,
CAST(ChargedParty AS SMALLINT),
REGEXP_REPLACE(ChargeNumber,'^F*|F$*','') AS CHARGED_PARTY_NUMBER,
CASE WHEN cleansedcharged = 0  THEN - 1 ELSE cleansedcharged END AS Cleansed_CHARGED_NUMBER,
internalCauseAndLoc,
trafficactivitycode,
disconnectingparty,
subscriptionType,
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
date_for_start_of_charge as call_timestamp_date,
if(length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END))=10,substr(cast((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END) as string), 1, 3),if(length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END))>10,
substr(substr(cast((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END) as string),length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END)) - 9, length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END))), 1, 3),NULL)) as Clng_Pty_NPA,
if(length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END))=10,substr(cast((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END) as string), 4, 3),if(length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END))>10,
substr(substr(cast((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END) as string),length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END)) - 9, length((CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END))), 4, 3),NULL)) as Clng_Pty_NXX,
if(length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END))=10,substr(cast((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END) as string), 1, 3),if(length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END))>10,
substr(substr(cast((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END) as string),length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END)) - 9, length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END))), 1, 3),NULL)) as cld_pty_NPA,
if(length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END))=10,substr(cast((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END) as string), 4, 3),if(length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END))>10,
substr(substr(cast((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END) as string),length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END)) - 9, length((CASE WHEN (RECORD_TYPE IN ('callForwarding') AND CleansedRedirecting <> cleansedCalled AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING))) AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE (CASE WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) ELSE CleansedCalled END)END))), 4, 3),NULL)) as cld_pty_NXX

FROM ext_cdrdm.gsm1205_transit_temp1 CDR;


INSERT INTO TABLE cdrdm.FACT_GSM_TRANSIT_CDR PARTITION (call_timestamp_date)
select 
cdr.Recordsequencenumber as record_sequence_number,
cdr.imsi,
cdr.exchangeidentity as exchange_identity,
cdr.switchidentity as switch_identity,
cdr.call_timestamp,
cdr.chargeable_duration,
cdr.calling_party_number,
cdr.cleansed_calling_number,
cdr.called_party_number,
cdr.cleansed_called_number,
cdr.original_called_number,
cdr.cleansed_original_number,
cdr.reg_seizure_charging_start,
cdr.redirecting_number,
cdr.cleansed_redirecting_number,
cdr.fault_code,
cdr.eosinfo,
cdr.incomingroute as incoming_route_id,
cdr.outgoingroute as outgoing_route_id,
cdr.chargedparty as charged_party,
cdr.charged_party_number,
cdr.cleansed_charged_number,
cdr.internalcauseandloc as internal_cause_and_loc,
cdr.trafficactivitycode as traffic_activity_code,
cdr.disconnectingparty as disconnecting_party,
cdr.subscriptiontype,
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
LERG6_calling.OCN as clng_pty_OCN,
LERG6_called.OCN as cld_pty_OCN,
LSMS_CALLING_PTY.SPID as Clng_Pty_LRN_SPID,
LSMS_CALLED_PTY.SPID as Cld_Pty_LRN_SPID,
cdr.call_timestamp_date
 
from ext_cdrdm.gsm1205_transit_temp2 CDR
LEFT OUTER JOIN
(Select z.calling_pty_NPA as NPA, z.calling_pty_NXX as NXX, z.calling_pty_OCN as OCN from (SELECT NPA as calling_pty_NPA, NXX calling_pty_NXX, OCN as calling_pty_OCN, row_number() over (partition by NPA, NXX order by OCN) as first_NPA_NPX_occurrence from ELA_V21.LERG6_NPA_NXX_BLOCKS) z where z.first_NPA_NPX_occurrence = 1) LERG6_calling
ON
CDR.Clng_Pty_NPA = LERG6_calling.NPA and CDR.Clng_Pty_NXX = LERG6_calling.NXX
LEFT OUTER JOIN
(Select z.called_pty_NPA as NPA, z.called_pty_NXX as NXX, z.called_pty_OCN as OCN from (SELECT NPA as called_pty_NPA, NXX called_pty_NXX, OCN as called_pty_OCN, row_number() over (partition by NPA, NXX order by OCN) as first_NPA_NPX_occurrence from ELA_V21.LERG6_NPA_NXX_BLOCKS) z where z.first_NPA_NPX_occurrence = 1) LERG6_called
ON
CDR.cld_pty_NPA = LERG6_called.NPA and CDR.cld_pty_NXX = LERG6_called.NXX
LEFT OUTER JOIN
(SELECT vv.SPID, vv.TN FROM (SELECT SPID, TN, row_number() over (partition by TN) FIRST_SPID  FROM ELA_V21.LSMS_REPLICA_A V)vv where vv.FIRST_SPID = 1) LSMS_CALLING_PTY
ON
CDR.cleansed_calling_number = LSMS_CALLING_PTY.TN

LEFT OUTER JOIN
(SELECT uu.SPID, uu.TN FROM (SELECT SPID, TN, row_number() over (partition by TN) FIRST_SPID  FROM ELA_V21.LSMS_REPLICA_A U)uu where uu.FIRST_SPID = 1) LSMS_CALLED_PTY
ON
CDR.cleansed_called_number = LSMS_CALLED_PTY.TN;



-------------------------------------CHANGES FOR LES MAIN BEGIN---------------------------------------------------------

INSERT INTO TABLE cdrdm.fact_gsm_trnst_cdr_stg    
SELECT
Recordsequencenumber,
(CASE WHEN (LENGTH(TRIM(SUBSTR(IMSI,1,15))) = 0  OR (SUBSTR(IMSI,1,15) IS NULL)) THEN -1 ELSE CAST(SUBSTR(IMSI,1,15) AS BIGINT) END) AS IMSI,
exchangeidentity,
switchIdentity,
CAST(CONCAT(date_for_start_of_charge,' ',SUBSTR (time_for_start_of_charge,1,8)) AS STRING) AS Call_Timestamp,
(CASE WHEN chargeableduration = '' THEN NULL ELSE (SUBSTR (chargeableduration,1,2) * 3600 + SUBSTR (chargeableduration,4,2) * 60 + SUBSTR (chargeableduration,7,2)) END) AS CHARGEABLE_DURATION,  
REGEXP_REPLACE(callingpartynumber,'^F*|F$*','') AS CALLING_PARTY_NUMBER,
CASE WHEN CleansedCalling = 0 THEN - 1 ELSE CleansedCalling END AS Cleansed_CALLING_NUMBER,
REGEXP_REPLACE(calledpartynumber,'^F*|F$*','') AS CALLED_PARTY_NUMBER,
(CASE 
  WHEN (RECORD_TYPE IN ('callForwarding') 
         AND CleansedRedirecting <> cleansedCalled 
         AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) <> LENGTH(TRIM(CAST(CleansedRedirecting AS STRING)))
         AND LENGTH(TRIM(CAST(CleansedCalled AS STRING))) > 6) 
  THEN
      (CASE  
        WHEN CleansedRedirecting = 0 THEN - 1 
        ELSE CleansedRedirecting END)
  ELSE
      (CASE  
        WHEN CleansedCalled = 0 THEN (CASE WHEN CleansedRedirecting = 0 THEN - 1 ELSE CleansedRedirecting END) 
        ELSE CleansedCalled END)
END) AS Cleansed_CALLED_NUMBER,
REGEXP_REPLACE(originalcallednumber,'^F*|F$*','') AS ORIGINAL_CALLED_NUMBER,
(CASE 
 WHEN (RECORD_TYPE IN ('callForwarding') AND cleansedOriginal = 0)
 THEN       
   (CASE  
       WHEN CleansedCalled = 0 THEN - 1
       ELSE CleansedCalled  
    END)
 ELSE
    (CASE  
       WHEN CleansedOriginal = 0 THEN - 1
       ELSE CleansedOriginal 
     END)
END) AS Cleansed_ORIGINAL_NUMBER,
(CASE WHEN timefortcseizure = '' THEN -1 
      ELSE (CAST(SUBSTR(timefortcseizure,1,2) AS INT)*3600
            +CAST(SUBSTR(timefortcseizure,3,2) AS INT)*60
            +CAST(SUBSTR(timefortcseizure,5,2) AS INT)) 
      END) AS reg_seizure_charging_start,
REGEXP_REPLACE(RedirectingNumber,'^F*|F$*','') AS REDIRECTING_NUMBER,
CASE WHEN COALESCE(CleansedRedirecting,0) = 0 THEN - 1 ELSE COALESCE(CleansedRedirecting,0) END AS Cleansed_REDIRECTING_NUMBER,
COALESCE(FaultCode,'') AS Fault_Code,
(CASE WHEN TRIM(trafficactivitycode) = '' THEN NULL ELSE EOSInfo END) AS EOSInfo,
(CASE WHEN TRIM(incomingroute) = '' THEN NULL ELSE TRIM(incomingroute) END) AS incomingroute,
(CASE WHEN TRIM(outgoingroute) = '' THEN NULL ELSE TRIM(outgoingroute) END) AS outgoingroute,
CAST(ChargedParty AS SMALLINT),
REGEXP_REPLACE(ChargeNumber,'^F*|F$*','') AS CHARGED_PARTY_NUMBER,
CASE WHEN cleansedcharged = 0  THEN - 1 ELSE cleansedcharged END AS Cleansed_CHARGED_NUMBER,
internalCauseAndLoc,
trafficactivitycode,
disconnectingparty,
subscriptionType,
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
date_for_start_of_charge as call_timestamp_date 
FROM ext_cdrdm.gsm1205_transit_temp1 CDR;
-------------------------------------CHANGES FOR LES MAIN END------------------------------------------------------------


DROP TABLE IF EXISTS ext_cdrdm.gsm1205_transit_temp1;
DROP TABLE IF EXISTS ext_cdrdm.gsm1205_transit_temp2;
