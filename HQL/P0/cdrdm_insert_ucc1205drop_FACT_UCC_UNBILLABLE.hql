--[Version History]
--0.1 - danchang - 4/06/2016 - Latest version from Alfaz, using test tables (e.g. cdrdm.test_ucc1205drop)
--0.2 - danchang - 4/25/2016 - Removed test specific references (cdrdm.test_ucc1205drop, and added back control fields)
--0.3 - saseenthar - 6/10/2016 - Updated new fields as part of UCC Phase 2 Requirement

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

INSERT INTO TABLE cdrdm.FACT_UCC_UNBILLABLE PARTITION (servicerequest_timestamp_normalized_date)
SELECT
trim(sip_method),
trim(role_of_node),
trim(session_id),
trim(calling_party_address),
trim(called_party_address),
if((record_opening_time is not NULL) and length(trim(record_opening_time)) > 0,CONCAT(SUBSTR(record_opening_time,1,4),'-',SUBSTR(record_opening_time,5,2),'-',SUBSTR(record_opening_time,7,2),' ',SUBSTR(record_opening_time,9,2),':',SUBSTR(record_opening_time,11,2),':',SUBSTR(record_opening_time,13,2)), NULL),
if((record_closure_time is not NULL) and length(trim(record_closure_time)) > 0,CONCAT(SUBSTR(record_closure_time,1,4),'-',SUBSTR(record_closure_time,5,2),'-',SUBSTR(record_closure_time,7,2),' ',SUBSTR(record_closure_time,9,2),':',SUBSTR(record_closure_time,11,2),':',SUBSTR(record_closure_time,13,2)), NULL),
if((servicerequest_timestamp_normalized is not NULL) and length(trim(servicerequest_timestamp_normalized)) > 0,CONCAT(SUBSTR(servicerequest_timestamp_normalized,1,4),'-',SUBSTR(servicerequest_timestamp_normalized,5,2),'-',SUBSTR(servicerequest_timestamp_normalized,7,2),' ',SUBSTR(servicerequest_timestamp_normalized,9,2),':',SUBSTR(servicerequest_timestamp_normalized,11,2),':',SUBSTR(servicerequest_timestamp_normalized,13,2)), NULL),
if((servicedeliverystart_timestamp_normalized is not NULL) and length(trim(servicedeliverystart_timestamp_normalized)) > 0,CONCAT(SUBSTR(servicedeliverystart_timestamp_normalized,1,4),'-',SUBSTR(servicedeliverystart_timestamp_normalized,5,2),'-',SUBSTR(servicedeliverystart_timestamp_normalized,7,2),' ',SUBSTR(servicedeliverystart_timestamp_normalized,9,2),':',SUBSTR(servicedeliverystart_timestamp_normalized,11,2),':',SUBSTR(servicedeliverystart_timestamp_normalized,13,2)), NULL),
if((servicedeliveryend_timestamp_normalized is not NULL) and length(trim(servicedeliveryend_timestamp_normalized)) > 0,CONCAT(SUBSTR(servicedeliveryend_timestamp_normalized,1,4),'-',SUBSTR(servicedeliveryend_timestamp_normalized,5,2),'-',SUBSTR(servicedeliveryend_timestamp_normalized,7,2),' ',SUBSTR(servicedeliveryend_timestamp_normalized,9,2),':',SUBSTR(servicedeliveryend_timestamp_normalized,11,2),':',SUBSTR(servicedeliveryend_timestamp_normalized,13,2)), NULL),
CAST(chargeable_duration AS BIGINT) AS chargeable_duration,
CAST(timefrom_siprequest_tostart_ofcharging AS BIGINT) AS timefrom_siprequest_tostart_ofcharging,
partial_output_recordnumber,
trim(incomplete_cdr_indication),
trim(ims_charging_identifier),
trim(accessnetwork_information),
trim(access_domain),
trim(access_type),
trim(ctn),
trim(imsi),
trim(imei),
trim(requested_party_address),
trim(mobilestation_roaming_number),
trim(tariff_class),
trim(error_codes),
supplementry_service_id,
CASE WHEN (channel_seizure_date_2 IS NOT NULL AND LENGTH(TRIM(channel_seizure_date_2)) > 0 ) THEN channel_seizure_date_2 ELSE NULL END AS channel_seizure_date_2,
media_type,
CAST(TRIM(voice_call_duration) AS BIGINT) AS voice_call_duration,
CAST(TRIM(video_call_duration) AS BIGINT) AS video_call_duration,
virtual_tn,
type_of_call,
DROP_type,
--phase 2 changes starts
trim(NetworkCallType) AS Network_Call_Type,
trim(CarrierIdCode) AS Carrier_Id_Code,
--phase 2 changes ends
coalesce(trim(ctn),NULL) as Subscriber_No,
if(substr(reverse(file_name),5,2) = 'BN','SF','MD') as File_Type,
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
concat(substr(servicerequest_timestamp_normalized,1,4),'-',substr(servicerequest_timestamp_normalized,5,2),'-',substr(servicerequest_timestamp_normalized,7,2)) as servicerequest_timestamp_normalized_date
FROM cdrdm.ucc1205drop a ${hiveconf:WHERE_clause};