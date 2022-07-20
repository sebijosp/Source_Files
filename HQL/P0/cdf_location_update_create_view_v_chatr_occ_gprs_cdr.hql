USE CDRDM;

DROP VIEW IF EXISTS v_chatr_occ_gprs_cdr;

CREATE VIEW IF NOT EXISTS v_chatr_occ_gprs_cdr AS 
SELECT
CAST(fact_chatr_occ_cdr.subscriber_no AS BIGINT) AS subscriber_no,
fact_chatr_occ_cdr.subscriber_no AS subscriber_no_char,
fact_chatr_occ_cdr.recordidentificationnumber AS record_sequence_number,
'18' AS gprs_choice_mask_archive,
IF(fact_chatr_occ_cdr.imsi is NULL, fact_chatr_occ_cdr.drvd_imsi, fact_chatr_occ_cdr.imsi) AS served_imsi,
fact_chatr_occ_cdr.triggerdate AS record_opening_date,
SUBSTR(fact_chatr_occ_cdr.triggertime, 12, 8) AS record_opening_time,
fact_chatr_occ_cdr.subscriber_no AS served_msisdn,
(fact_chatr_occ_cdr.uplink_octets_unit1 + fact_chatr_occ_cdr.uplink_octets_unit2 + fact_chatr_occ_cdr.uplink_octets_unit3) AS data_volume_uplink_archive,
(fact_chatr_occ_cdr.downlink_octets_unit1 + fact_chatr_occ_cdr.downlink_octets_unit2 + fact_chatr_occ_cdr.downlink_octets_unit3) AS data_volume_downlink_archive,
fact_chatr_occ_cdr.gy_routing_area_code AS routing_area,
fact_chatr_occ_cdr.gy_location_area_code AS location_area_code,
fact_chatr_occ_cdr.gy_called_stat_id AS access_point_name,
'' AS reporting_centre_id,
'' AS destination_url,
'0365' AS spid,
'' AS service_class_group,
'' AS content_delivered,
fact_chatr_occ_cdr.cc_serviceidentifier AS event_protocol_type,
CASE WHEN fact_chatr_occ_cdr.gy_rat_type = '1' THEN 'UTRAN'
WHEN fact_chatr_occ_cdr.gy_rat_type = '2' THEN 'GERAN'
WHEN fact_chatr_occ_cdr.gy_rat_type = '3' THEN 'WLAN'
WHEN fact_chatr_occ_cdr.gy_rat_type = '4' THEN 'GAN'
WHEN fact_chatr_occ_cdr.gy_rat_type = '5' THEN 'HSPA Evolution'
WHEN fact_chatr_occ_cdr.gy_rat_type = '6' THEN 'EUTRAN'
ELSE '' END AS wireless_generation,
'' AS event_count,
'' AS domain_1,
'O' AS cdr_type_ind,
fact_chatr_occ_cdr.gy_imeisv AS served_imei,
fact_chatr_occ_cdr.gy_sgsn_address AS sgsn_address,
fact_chatr_occ_cdr.gy_pdp_address AS served_pdp_address,
fact_chatr_occ_cdr.gy_location_mcc_mnc AS plmn_id,
unix_timestamp(fact_chatr_occ_cdr.cc_eve_datetime) - unix_timestamp(fact_chatr_occ_cdr.triggertime) AS duration,
fact_chatr_occ_cdr.gy_charging_id AS charging_id,
IF (chatr_zoneid.zone_id_hex is NULL, NVL(fact_chatr_occ_cdr.gy_cell_identity, fact_chatr_occ_cdr.gy_service_area_code), fact_chatr_occ_cdr.gy_service_area_code) AS cell_id,
chatr_zoneid.zone_id_hex AS zone_id,
fact_chatr_occ_cdr.cc_roaming_pos AS customer_type,
'' AS monum,
fact_chatr_occ_cdr.gy_tracking_area_code AS tracking_area_code,
fact_chatr_occ_cdr.gy_ecid AS eutran_cellid,
fact_chatr_occ_cdr.triggertime_offset AS rec_opening_dt_offset,
fact_chatr_occ_cdr.file_name,
fact_chatr_occ_cdr.record_start,
fact_chatr_occ_cdr.record_end,
fact_chatr_occ_cdr.record_type,
fact_chatr_occ_cdr.family_name,
fact_chatr_occ_cdr.version_id,
fact_chatr_occ_cdr.file_time,
fact_chatr_occ_cdr.file_id,
fact_chatr_occ_cdr.switch_name,
fact_chatr_occ_cdr.num_records,
fact_chatr_occ_cdr.insert_timestamp
FROM cdrdm.fact_chatr_occ_cdr fact_chatr_occ_cdr
LEFT OUTER JOIN cdrdm.chatr_zoneid chatr_zoneid
ON fact_chatr_occ_cdr.gy_cell_identity = chatr_zoneid.zone_id_hex

UNION ALL
SELECT
fact_gprs_cdr.subscriber_no,
fact_gprs_cdr.subscriber_no_char,
fact_gprs_cdr.record_sequence_number,
fact_gprs_cdr.gprs_choice_mask_archive,
fact_gprs_cdr.served_imsi,
fact_gprs_cdr.record_opening_date,
fact_gprs_cdr.record_opening_time,
fact_gprs_cdr.served_msisdn,
fact_gprs_cdr.data_volume_uplink_archive,
fact_gprs_cdr.data_volume_downlink_archive,
fact_gprs_cdr.routing_area,
fact_gprs_cdr.location_area_code,
fact_gprs_cdr.access_point_name,
fact_gprs_cdr.reporting_centre_id,
fact_gprs_cdr.destination_url,
fact_gprs_cdr.spid,
fact_gprs_cdr.service_class_group,
fact_gprs_cdr.content_delivered,
fact_gprs_cdr.event_protocol_type,
fact_gprs_cdr.wireless_generation,
fact_gprs_cdr.event_count,
fact_gprs_cdr.domain_1,
fact_gprs_cdr.cdr_type_ind,
fact_gprs_cdr.served_imei,
fact_gprs_cdr.sgsn_address,
fact_gprs_cdr.served_pdp_address,
fact_gprs_cdr.plmn_id,
fact_gprs_cdr.duration,
fact_gprs_cdr.charging_id,
fact_gprs_cdr.cell_id,
NULL as zone_id,
fact_gprs_cdr.customer_type,
fact_gprs_cdr.monum,
fact_gprs_cdr.tracking_area_code,
fact_gprs_cdr.eutran_cellid,
fact_gprs_cdr.rec_opening_dt_offset,
fact_gprs_cdr.file_name,
fact_gprs_cdr.record_start,
fact_gprs_cdr.record_end,
fact_gprs_cdr.record_type,
fact_gprs_cdr.family_name,
fact_gprs_cdr.version_id,
fact_gprs_cdr.file_time,
fact_gprs_cdr.file_id,
fact_gprs_cdr.switch_name,
fact_gprs_cdr.num_records,
fact_gprs_cdr.insert_timestamp
FROM
cdrdm.fact_gprs_cdr