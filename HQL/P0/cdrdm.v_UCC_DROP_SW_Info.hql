--[Version History]
--0.1 - saseenthar - 6/14/2016 - intial version as part of UCC Phase 2 Requirement
--creates cdrdm.v_UCC_DROP_SW_Info VIEW data using cdrdm.FACT_UCC_UNBILLABLE and cdrdm.fact_ucc_cdr based on session_id and latest RecordOpeningTime

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

DROP VIEW IF EXISTS v_UCC_DROP_SW_Info;

CREATE VIEW IF NOT EXISTS v_UCC_DROP_SW_Info AS
SELECT
a.sip_method,
a.role_of_node,
a.session_id,
a.calling_party_address,
a.called_party_address,
a.record_opening_time,
a.record_closure_time,
a.servicerequest_timestamp_normalized,
a.servicedeliverystart_timestamp_normalized,
a.servicedeliveryend_timestamp_normalized,
a.chargeable_duration,
a.timefrom_siprequest_tostart_of_charging,
a.partial_output_record_number,
a.incomplete_cdr_indication,
a.ims_charging_identifier,
a.accessnetwork_information,
a.access_domain,
a.access_type,
a.CTN,
a.IMSI,
a.IMEI,
a.requested_party_address,
a.mobilestation_roaming_number,
a.tariff_class,
a.error_codes,
a.supplementry_service_id,
a.channel_seizure_date_2,
a.media_type,
a.voice_call_duration,
a.video_call_duration,
a.virtual_tn,
a.type_of_call,
a.DROP_type,
a.Network_Call_Type,
a.Carrier_Id_Code,
a.Subscriber_No,
a.File_Type,
a.file_name,
a.record_start,
a.record_end,
a.record_type,
a.family_name,
a.version_id,
a.file_time,
a.file_id,
c.switch_name AS switch_name,
a.num_records,
a.insert_timestamp
FROM cdrdm.FACT_UCC_UNBILLABLE a
JOIN
(
SELECT b.sessionid, b.switch_name
FROM
( SELECT sessionid, switch_name, 
ROW_NUMBER() OVER(PARTITION BY sessionid, switch_name ORDER BY RecordOpeningTime DESC) AS latest_switch
FROM cdrdm.fact_ucc_cdr) b where b.latest_switch = 1 
) c
on
a.session_id = c.sessionid
;