--[Version History]
--0.1 - danchang - 4/13/2016 - initial version
--
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

DROP VIEW IF EXISTS v_chatr_occ_gprs_cdr;

--Create view by UNION of FACT_CHATR_OCC_CDR and FACT_GPRS_CDR
CREATE VIEW IF NOT EXISTS v_chatr_occ_gprs_cdr AS
SELECT
--The first half of the select is for fields from FACT_CHATR_OCC_CDR
pmod(CAST(subscriber_no AS BIGINT),10000000000) AS subscriber_no,
subscriber_no AS subscriber_no_char,
recordIdentificationNumber AS record_sequence_number,
'18' AS gprs_choice_mask_archive, 
IF(IMSI is NULL, drvd_IMSI, IMSI) AS served_imsi,
--Trigger Time format YYYY-MM-DD HR:Mi:SS
triggerDate AS record_opening_date,
SUBSTR(triggerTime, 12, 8) AS record_opening_time,
Subscriber_no AS served_msisdn,
(NVL(uplink_Octets_Unit1,0) + NVL(uplink_Octets_Unit2,0) + NVL(uplink_Octets_Unit3,0)) AS data_volume_uplink_archive,
(NVL(downlink_Octets_Unit1,0) + NVL(downlink_Octets_Unit2,0) + NVL(downlink_Octets_Unit3,0)) AS data_volume_downlink_archive,
Gy_Routing_Area_Code AS routing_area,
Gy_Location_Area_Code AS location_area_code,
Gy_Called_Stat_Id AS access_point_name,
'' AS reporting_centre_id,
'' AS destination_url,
'0365' AS spid,
'' AS service_class_group,
'' AS content_delivered,
cc_serviceIdentifier AS event_protocol_type,
CASE WHEN Gy_RAT_Type = '1' THEN 'UTRAN'
WHEN Gy_RAT_Type = '2' THEN 'GERAN'
WHEN Gy_RAT_Type = '3' THEN 'WLAN'
WHEN Gy_RAT_Type = '4' THEN 'GAN'
WHEN Gy_RAT_Type = '5' THEN 'HSPA Evolution'
WHEN Gy_RAT_Type = '6' THEN 'EUTRAN'
ELSE '' END AS wireless_generation,
'' AS event_count,
'' AS domain_1,
'O' AS cdr_type_ind,
Gy_IMEISV AS served_imei,
Gy_SGSN_Address AS sgsn_address,
Gy_PDP_Address AS served_pdp_address,
Gy_Location_MCC_MNC AS plmn_id,
unix_timestamp(cc_eve_datetime) - unix_timestamp(triggerTime) AS duration,
Gy_Charging_Id AS charging_id,
if (Gy_Cell_Identity is NULL, Gy_Service_Area_Code, Gy_Cell_Identity) AS cell_id,
cc_roaming_pos AS customer_type,
'' AS monum,
Gy_Tracking_Area_Code AS tracking_area_code,
Gy_ECID AS eutran_cellid,
triggerTime_offset AS rec_opening_dt_offset,
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
insert_timestamp
FROM fact_chatr_occ_cdr

UNION ALL
-- Second half of the select for fields from FACT_GPRS_CDR
SELECT
subscriber_no,
subscriber_no_char,
CAST(record_sequence_number AS varchar(255)),
gprs_choice_mask_archive,
served_imsi,
record_opening_date,
record_opening_time,
served_msisdn,
data_volume_uplink_archive,
data_volume_downlink_archive,
routing_area,
location_area_code,
access_point_name,
CAST(reporting_centre_id AS STRING),
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
CAST(event_count AS STRING),
domain_1,
cdr_type_ind,
served_imei,
sgsn_address,
served_pdp_address,
plmn_id,
duration,
charging_id,
cell_id,
customer_type,
monum,
tracking_area_code,
eutran_cellid,
rec_opening_dt_offset,
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
insert_timestamp
FROM
fact_gprs_cdr;
