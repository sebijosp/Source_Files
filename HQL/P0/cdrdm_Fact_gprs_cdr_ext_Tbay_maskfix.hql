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

DROP VIEW IF EXISTS Fact_gprs_cdr_ext_Tbay;

CREATE VIEW IF NOT EXISTS Fact_gprs_cdr_ext_Tbay AS
SELECT 
(CASE 
WHEN (CASE WHEN (Served_IMSI BETWEEN 302720392000000 AND 302720392999999 OR Served_IMSI BETWEEN 302720592000000 AND 302720592999999 OR Served_IMSI BETWEEN 302720692000000 AND 302720692999999) THEN 'T' ELSE (CASE WHEN find_in_set(SUBSTR(TRIM(Served_IMSI),1,6), '302720,302370,302820') > 0 THEN 'R' ELSE 'O' END) END) = 'T' AND (CASE WHEN (tracking_area_code IS NULL OR TRIM(tracking_area_code) ='') AND (Location_area_code IS NULL OR TRIM(Location_area_code) ='') THEN NULL WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') > 0 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') > 0) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0) THEN 'T' WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') < 1 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') < 1) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0 OR TRIM(PLMN_ID) = '' OR PLMN_ID IS NULL) THEN 'R' ELSE 'O' END) = 'T' THEN 'D'
WHEN (CASE WHEN (Served_IMSI BETWEEN 302720392000000 AND 302720392999999 OR Served_IMSI BETWEEN 302720592000000 AND 302720592999999 OR Served_IMSI BETWEEN 302720692000000 AND 302720692999999) THEN 'T' ELSE (CASE WHEN find_in_set(SUBSTR(TRIM(Served_IMSI),1,6), '302720,302370,302820') > 0 THEN 'R' ELSE 'O' END) END) = 'O' AND (CASE WHEN (tracking_area_code IS NULL OR TRIM(tracking_area_code) ='') AND (Location_area_code IS NULL OR TRIM(Location_area_code) ='') THEN NULL WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') > 0 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') > 0) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0) THEN 'T' WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') < 1 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') < 1) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0 OR TRIM(PLMN_ID) = '' OR PLMN_ID IS NULL) THEN 'R' ELSE 'O' END) = 'T' THEN 'O'
WHEN (CASE WHEN (Served_IMSI BETWEEN 302720392000000 AND 302720392999999 OR Served_IMSI BETWEEN 302720592000000 AND 302720592999999 OR Served_IMSI BETWEEN 302720692000000 AND 302720692999999) THEN 'T' ELSE (CASE WHEN find_in_set(SUBSTR(TRIM(Served_IMSI),1,6), '302720,302370,302820') > 0 THEN 'R' ELSE 'O' END) END) = 'R' AND (CASE WHEN (tracking_area_code IS NULL OR TRIM(tracking_area_code) ='') AND (Location_area_code IS NULL OR TRIM(Location_area_code) ='') THEN NULL WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') > 0 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') > 0) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0) THEN 'T' WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') < 1 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') < 1) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0 OR TRIM(PLMN_ID) = '' OR PLMN_ID IS NULL) THEN 'R' ELSE 'O' END) = 'T' THEN 'R'
ELSE NULL
END) as IsTbayTel,
(CASE WHEN
(Served_IMSI BETWEEN 302720392000000 AND 302720392999999 OR-- HSPA
Served_IMSI BETWEEN 302720592000000 AND 302720592999999 OR-- LTE
Served_IMSI BETWEEN 302720692000000 AND 302720692999999)  -- LTE
THEN 'T'
ELSE (CASE WHEN SUBSTR(TRIM(Served_IMSI),1,6) IN ('302720','302370','302820') THEN 'R' ELSE 'O' END)
END) as IMSI_Type,
(CASE 
WHEN (tracking_area_code IS NULL OR TRIM(tracking_area_code) ='') AND (Location_area_code IS NULL OR TRIM(Location_area_code) ='') THEN NULL
WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') > 0 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') > 0) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0) THEN 'T'
WHEN (find_in_set(TRIM(UPPER(tracking_area_code)), '8791,87F5') < 1 OR find_in_set(TRIM(UPPER(Location_area_code)), 'FCBC,FD20,FCC1,FD25') < 1) AND (find_in_set(TRIM(PLMN_ID), '302720,302370,302820') > 0 OR TRIM(PLMN_ID) = '' OR PLMN_ID IS NULL) THEN 'R'
ELSE 'O'
END) as LAC_TAC_Type,
(CASE WHEN wireless_generation IS NULL THEN concat(COALESCE(PLMN_ID,'302720'), COALESCE(REGEXP_EXTRACT(TRIM(Location_area_code), '(0*)(.*)', 2),''), COALESCE(REGEXP_EXTRACT(TRIM(Cell_ID), '(0*)(.*)', 2),''))
WHEN find_in_set(wireless_generation, '3G,2G') > 0 AND (Location_area_code IS NULL OR Cell_Id IS NULL OR TRIM (Location_area_code) = '' OR TRIM(cell_id) ='' OR Cell_Id = '0000' OR Location_area_code = '0000') THEN concat(COALESCE(PLMN_ID,'302720'), REGEXP_EXTRACT(TRIM(Tracking_area_code), '(0*)(.*)', 2), REGEXP_EXTRACT(TRIM(Eutran_cellid), '(0*)(.*)', 2))
WHEN find_in_set(wireless_generation, '3G,2G') > 0 AND (Location_area_code IS NOT NULL AND Cell_Id IS NOT NULL AND TRIM(Location_area_code) <> '' AND TRIM(cell_id) <> '' AND Cell_Id <> '0000' AND Location_area_code <> '0000') THEN concat(COALESCE(PLMN_ID,'302720'), REGEXP_EXTRACT(TRIM(Location_area_code), '(0*)(.*)', 2), REGEXP_EXTRACT(TRIM(Cell_ID), '(0*)(.*)', 2))
WHEN find_in_set(wireless_generation, '4G,5G') > 0 AND (Tracking_area_code IS NULL OR Eutran_cellid IS NULL OR TRIM (Tracking_area_code) = '' OR TRIM(Eutran_cellid) ='' OR Tracking_area_code = '0000' OR Eutran_cellid = '00000000') THEN concat(COALESCE(PLMN_ID,'302720'), REGEXP_EXTRACT(TRIM(Location_area_code), '(0*)(.*)', 2), REGEXP_EXTRACT(TRIM(Cell_ID), '(0*)(.*)', 2))
WHEN find_in_set(wireless_generation, '4G,5G') > 0 AND (Tracking_area_code IS NOT NULL AND  Eutran_cellid IS NOT NULL OR TRIM(Tracking_area_code) <> '' AND TRIM(Eutran_cellid) <> '' AND Tracking_area_code <> '0000' AND Eutran_cellid <> '00000000') THEN concat(COALESCE(PLMN_ID,'302720'), REGEXP_EXTRACT(TRIM(Tracking_area_code), '(0*)(.*)', 2), REGEXP_EXTRACT(TRIM(Eutran_cellid), '(0*)(.*)', 2))
ELSE 'N-A'
END) as CID,
subscriber_no,
subscriber_no_char,
record_sequence_number,
trim(gprs_choice_mask_archive) AS gprs_choice_mask_archive,
served_imsi,
record_opening_date,
record_opening_time,
served_msisdn,
data_volume_uplink_archive,
data_volume_downlink_archive,
routing_area,
location_area_code,
access_point_name,
reporting_centre_id,
destination_url,
spid,
service_class_group,
content_delivered,
event_protocol_type,
wireless_generation,
event_count,
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

FROM v_chatr_occ_gprs_cdr a;