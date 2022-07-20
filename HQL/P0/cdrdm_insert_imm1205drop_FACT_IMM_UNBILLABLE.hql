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

INSERT INTO TABLE cdrdm.FACT_IMM_UNBILLABLE PARTITION (SrvcRequestDttmNrml_Date)
SELECT
trim(SIPMethod),
trim(RoleOfNode),
trim(SessionId),
trim(CallingPartyAddress),
trim(CalledPartyAddress),
if((RecordOpeningTime is not NULL) and length(trim(RecordOpeningTime)) > 0,CONCAT(SUBSTR(RecordOpeningTime,1,4),'-',SUBSTR(RecordOpeningTime,5,2),'-',SUBSTR(RecordOpeningTime,7,2),' ',SUBSTR(RecordOpeningTime,9,2),':',SUBSTR(RecordOpeningTime,11,2),':',SUBSTR(RecordOpeningTime,13,2)), NULL), --file_date
if((RecordClosureTime is not NULL) and length(trim(RecordClosureTime)) > 0,CONCAT(SUBSTR(RecordClosureTime,1,4),'-',SUBSTR(RecordClosureTime,5,2),'-',SUBSTR(RecordClosureTime,7,2),' ',SUBSTR(RecordClosureTime,9,2),':',SUBSTR(RecordClosureTime,11,2),':',SUBSTR(RecordClosureTime,13,2)), NULL),
if((SrvcRequestDttmNrml is not NULL) and length(trim(SrvcRequestDttmNrml)) > 0,CONCAT(SUBSTR(SrvcRequestDttmNrml,1,4),'-',SUBSTR(SrvcRequestDttmNrml,5,2),'-',SUBSTR(SrvcRequestDttmNrml,7,2),' ',SUBSTR(SrvcRequestDttmNrml,9,2),':',SUBSTR(SrvcRequestDttmNrml,11,2),':',SUBSTR(SrvcRequestDttmNrml,13,2)), NULL),
if((SrvcDeliveryStartDttmNrml is not NULL) and length(trim(SrvcDeliveryStartDttmNrml)) > 0,CONCAT(SUBSTR(SrvcDeliveryStartDttmNrml,1,4),'-',SUBSTR(SrvcDeliveryStartDttmNrml,5,2),'-',SUBSTR(SrvcDeliveryStartDttmNrml,7,2),' ',SUBSTR(SrvcDeliveryStartDttmNrml,9,2),':',SUBSTR(SrvcDeliveryStartDttmNrml,11,2),':',SUBSTR(SrvcDeliveryStartDttmNrml,13,2)), NULL),
if((serviceDeliveryEndDttmNrml is not NULL) and length(trim(serviceDeliveryEndDttmNrml)) > 0,CONCAT(SUBSTR(serviceDeliveryEndDttmNrml,1,4),'-',SUBSTR(serviceDeliveryEndDttmNrml,5,2),'-',SUBSTR(serviceDeliveryEndDttmNrml,7,2),' ',SUBSTR(serviceDeliveryEndDttmNrml,9,2),':',SUBSTR(serviceDeliveryEndDttmNrml,11,2),':',SUBSTR(serviceDeliveryEndDttmNrml,13,2)), NULL),
ChargeableDuration,
tmFromSipRqstStartOfChrgng,
PartialOutputRecordNumber,
trim(IncompleteCDRIndications),
trim(IMSChargingIdentifier),
trim(AccessNetworkInfo1),
trim(AccessDomain1),
trim(AccessType1),
trim(CTN),
trim(SubIMSI),
trim(SubIMEI),
trim(RequestedPartyAddress),
trim(MobileStationRoamingNumber),
TariffClass,
trim(ERRORCODE),
If((CTN is not NULL) and length(CTN) > 0,cast(CTN as BIGINT),NULL) as SubscriberNo,
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
if (SrvcRequestDttmNrml is not NULL,concat(substr(SrvcRequestDttmNrml,1,4),'-',substr(SrvcRequestDttmNrml,5,2),'-',substr(SrvcRequestDttmNrml,7,2)),NULL) as SrvcRequestDttmNrml_Date
FROM cdrdm.imm1205drop a ${hiveconf:WHERE_clause};