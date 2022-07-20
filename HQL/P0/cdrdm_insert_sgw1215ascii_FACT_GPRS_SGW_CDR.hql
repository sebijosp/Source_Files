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

INSERT INTO TABLE cdrdm.FACT_GPRS_SGW_CDR PARTITION (recordOpeningDate)
SELECT
recordType,
servedIMSI,
s_GWAddressAddress,
chargingID,
servingNodeAddress1,
servingNodeAddress2,
servingNodeAddress3,
servingNodeAddress4,
servingNodeAddress5,
pdpPDNType,
servedPDPPDNAddress,
dataVolumeGPRSUplink1,
dataVolumeGPRSDownlink1,
changeCondition1,
if (changeTime1 IS NOT NULL and length(trim(changeTime1)) > 0, CONCAT ('20',substr(changeTime1,1,8),' ',substr(changeTime1,10,2),':',substr(changeTime1,13,2),':',substr(changeTime1,16,2)),NULL),
userLocationInformation1,
qCI1,
maxRequestedBandwithUL1,
maxRequestedBandwithDL1,
guaranteeBitrateUL1,
guaranteeBitrateDL1,
aRP1,
dataVolumeGPRSUplink2,
dataVolumeGPRSDownlink2,
changeCondition2,
if (changeTime2 IS NOT NULL and length(trim(changeTime2)) > 0,CONCAT ('20',substr(changeTime2,1,8),' ',substr(changeTime2,10,2),':',substr(changeTime2,13,2),':',substr(changeTime2,16,2)),NULL),
userLocationInformation2,
qCI2,
maxRequestedBandwithUL2,
maxRequestedBandwithDL2,
guaranteeBitrateUL2,
guaranteeBitrateDL2,
aRP2,
dataVolumeGPRSUplink3,
dataVolumeGPRSDownlink3,
changeCondition3,
if (changeTime3 IS NOT NULL and length(trim(changeTime3)) > 0,CONCAT ('20',substr(changeTime3,1,8),' ',substr(changeTime3,10,2),':',substr(changeTime3,13,2),':',substr(changeTime3,16,2)),NULL),
userLocationInformation3,
qCI3,
maxRequestedBandwithUL3,
maxRequestedBandwithDL3,
guaranteeBitrateUL3,
guaranteeBitrateDL3,
aRP3,
dataVolumeGPRSUplink4,
dataVolumeGPRSDownlink4,
changeCondition4,
if (changeTime4 IS NOT NULL and length(trim(changeTime4)) > 0,CONCAT ('20',substr(changeTime4,1,8),' ',substr(changeTime4,10,2),':',substr(changeTime4,13,2),':',substr(changeTime4,16,2)),NULL),
userLocationInformation4,
qCI4,
maxRequestedBandwithUL4,
maxRequestedBandwithDL4,
guaranteeBitrateUL4,
guaranteeBitrateDL4,
aRP4,
dataVolumeGPRSUplink5,
dataVolumeGPRSDownlink5,
changeCondition5,
if (changeTime5 IS NOT NULL and length(trim(changeTime5)) > 0,CONCAT ('20',substr(changeTime5,1,8),' ',substr(changeTime5,10,2),':',substr(changeTime5,13,2),':',substr(changeTime5,16,2)),NULL),
userLocationInformation5,
qCI5,
maxRequestedBandwithUL5,
maxRequestedBandwithDL5,
guaranteeBitrateUL5,
guaranteeBitrateDL5,
aRP5,
if (recordOpeningTime IS NOT NULL and length(trim(recordOpeningTime)) > 0,CONCAT ('20',substr(recordOpeningTime,1,8),' ',substr(recordOpeningTime,10,2),':',substr(recordOpeningTime,13,2),':',substr(recordOpeningTime,16,2)),NULL),
duration,
causeForRecClosing,
recordSequenceNumber,
localSequenceNumber,
servedMSISDN,
chargingCharacteristics,
servingNodePLMNIdentifier,
rATType,
mSTimeZone,
sGWChange,
servingNodeType1,
servingNodeType2,
servingNodeType3,
servingNodeType4,
servingNodeType5,
p_GWAddressUsed,
p_GWPLMNIdentifier,
pDNConnectionID,
APN,
Served_IMEI,
if(userLocationInformation1 is not NULL and length(trim(userLocationInformation1)) > 0 and length(trim(substr(userLocationInformation1,12,7)))>0 ,concat(trim(substr(userLocationInformation1,1,3)),trim(substr(userLocationInformation1,4,3)),trim(substr(userLocationInformation1,7,5)),conv(concat(conv(trim(substring(userLocationInformation1,12,7)),10,16),'0',trim(substring(userLocationInformation1,19,5))),16,10)),NULL) as ECID,
if(userLocationInformation1 is not NULL and length(trim(userLocationInformation1)) > 0 and length(trim(substr(userLocationInformation1,12,7)))>0 ,conv(trim(substr(userLocationInformation1,7,5)),10,16),NULL) as TAC_HEX,
if(userLocationInformation1 is not NULL and length(trim(userLocationInformation1)) > 0 and length(trim(substr(userLocationInformation1,12,7)))>0 ,lpad(concat(conv(trim(substr(userLocationInformation1,12,7)),10,16),'0',trim(substr(userLocationInformation1,19,5))),8,'0'),NULL) as ECID_HEX,
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
if (recordOpeningTime IS NOT NULL and length(trim(recordOpeningTime)) > 0, CONCAT ('20',substr(recordOpeningTime,1,2),'-',substr(recordOpeningTime,4,2),'-',substr(recordOpeningTime,7,2)),NULL) as recordOpeningDate
FROM cdrdm.sgw1215ascii a ${hiveconf:WHERE_clause};
