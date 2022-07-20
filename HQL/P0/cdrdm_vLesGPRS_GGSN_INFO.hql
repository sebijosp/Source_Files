SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;

CREATE VIEW IF NOT EXISTS cdrdm.vLesGPRS_GGSN_INFO AS
SELECT 
a.subscriber_no,
--a.subscriber_no_char,
cast(a.record_sequence_number as int) as record_sequence_number,
a.gprs_choice_mask_archive,
a.served_imsi,
a.record_opening_date,
a.record_opening_time,
a.rec_opening_dt_offset,
a.served_msisdn,
sum(NVL(a.data_volume_uplink_archive,0)) as data_volume_uplink_archive,
sum(NVL(a.data_volume_downlink_archive,0)) as data_volume_downlink_archive,
a.routing_area,
a.location_area_code,
a.access_point_name,
--a.time_key,
a.switch_name, --switch_name instead of switch_id
--a.reporting_centre_id,
--a.eng_date_id,
--a.audit_key,
a.destination_url,
a.spid,
a.service_class_group,
a.content_delivered,
a.event_protocol_type,
a.wireless_generation,
sum(NVL(a.event_count,0)) as event_count,
a.domain_1,
a.cdr_type_ind,
a.served_imei,
a.sgsn_address,
a.served_pdp_address,
a.plmn_id,
sum(NVL(a.duration,0)) as duration,
a.charging_id,
a.cell_id,
a.customer_type,
--a.monum,
a.tracking_area_code,
a.eutran_cellid,
--a.IsTbayTel,
NULL AS user_location_information_1_orig,
a.cid, 
b.file_type,
b.SITE,
b.CELL,
b.ANTENNA_TY,
b.AZIMUTH,
b.BEAMWIDTH,
b.SITE_NAME AS Cell_site_name,
b.CITY AS Cell_site_City_name,
b.PROVINCE AS Cell_site_Prov_Name,
b.ADDRESS AS Cell_site_Address,
b.X AS Cell_site_X,
b.Y AS Cell_site_Y,
b.LATITUDE AS Cell_site_Latitude,
b.LONGITUDE AS Cell_site_Longitude,
b.CGI AS Cell_decimal,
b.file_date,
'GPRS' as GPRS_SOURCE 
FROM cdrdm.Fact_gprs_cdr_ext_Tbay a LEFT OUTER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) CGI_CELL FROM cdrdm.DIM_CELL_SITE_INFO) b ON a.cid = TRIM(b.CGI_HEX)
WHERE (CGI_CELL = 1 OR CGI_CELL IS NULL)
GROUP BY a.subscriber_no, a.served_imsi, a.record_opening_date, a.record_opening_time, a.location_area_code, a.access_point_name, a.switch_name, a.destination_url, a.spid,
a.service_class_group, a.content_delivered, a.event_protocol_type, a.wireless_generation, a.domain_1, a.cdr_type_ind, a.served_imei, a.sgsn_address,
a.served_pdp_address, a.plmn_id, a.cell_id, a.served_msisdn, a.record_sequence_number, a.gprs_choice_mask_archive, a.routing_area, a.charging_id, a.customer_type,
a.tracking_area_code, a.eutran_cellid, a.cid, b.file_type, b.SITE, b.CELL, b.ANTENNA_TY, b.AZIMUTH, b.BEAMWIDTH, b.SITE_NAME, b.CITY, b.PROVINCE,
b.ADDRESS, b.X, b.Y, b.LATITUDE, b.LONGITUDE, b.CGI, b.file_date, a.rec_opening_dt_offset
union all
select 
ggsn.subscriber_no,
ggsn.record_sequence_number,
NULL as gprs_choice_mask_archive,
ggsn.served_imsi,
ggsn.record_opening_date,
substr(ggsn.record_opening_time,12,8) as record_opening_time,
NULL as rec_opening_dt_offset,
ggsn.served_msisdn,
sum(NVL(ggsn.data_volume_gprs_uplink_1,0)+NVL(ggsn.data_volume_gprs_uplink_2,0)+NVL(ggsn.data_volume_gprs_uplink_3,0)+NVL(ggsn.data_volume_gprs_uplink_4,0)+NVL(ggsn.data_volume_gprs_uplink_5,0)) as data_volume_uplink_archive,
sum(NVL(ggsn.data_volume_gprs_downlink_1,0)+NVL(ggsn.data_volume_gprs_downlink_2,0)+NVL(ggsn.data_volume_gprs_downlink_3,0)+NVL(ggsn.data_volume_gprs_downlink_4,0)+NVL(ggsn.data_volume_gprs_downlink_5,0)) as data_volume_downlink_archive,
NULL as routing_area,
NULL as location_area_code,
ggsn.access_point_name_ni as access_point_name,
ggsn.switch_name,
NULL as destination_url,
NULL as spid,
NULL as service_class_group,
NULL as content_delivered,
NULL as event_protocol_type,
NULL as wireless_generation,
NULL as event_count,
NULL as domain_1,
NULL as cdr_type_ind,
ggsn.served_imei_sv as served_imei,
ggsn.sgsn_address_1 as sgsn_address,
ggsn.served_pdp_address,
NULL as plmn_id,
sum(NVL(ggsn.duration,0)) as duration,
ggsn.charging_id,
NULL as cell_id,
NULL as customer_type,
NULL as tracking_area_code,
NULL as eutran_cellid,
ggsn.user_location_information_1_orig,
TRIM(dcsi.CGI_HEX) as cid, 
dcsi.file_type,
dcsi.SITE,
dcsi.CELL,
dcsi.ANTENNA_TY,
dcsi.AZIMUTH,
dcsi.BEAMWIDTH,
dcsi.SITE_NAME as Cell_site_name,
dcsi.CITY AS Cell_site_City_name,
dcsi.PROVINCE AS Cell_site_Prov_Name,
dcsi.ADDRESS AS Cell_site_Address,
dcsi.X AS Cell_site_X,
dcsi.Y AS Cell_site_Y,
dcsi.LATITUDE AS Cell_site_Latitude,
dcsi.LONGITUDE AS Cell_site_Longitude,
dcsi.CGI AS Cell_decimal,
dcsi.file_date,
'MPG_GGSN' AS GPRS_SOURCE
FROM cdrdm.fact_gprs_mpg_ggsn_cdr ggsn LEFT OUTER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) CGI_CELL FROM cdrdm.DIM_CELL_SITE_INFO) dcsi ON 
regexp_replace(ggsn.user_location_information_1_orig," |-",'')=regexp_replace(dcsi.cgi," |-",'')
WHERE (CGI_CELL = 1 OR CGI_CELL IS NULL)
GROUP BY ggsn.subscriber_no, ggsn.served_imsi, ggsn.record_opening_date, ggsn.record_opening_time, ggsn.access_point_name_ni, ggsn.switch_name, ggsn.served_imei_sv, ggsn.sgsn_address_1,
ggsn.served_pdp_address, ggsn.served_msisdn, ggsn.record_sequence_number,  ggsn.charging_id,ggsn.user_location_information_1_orig,TRIM(dcsi.CGI_HEX),
dcsi.file_type, dcsi.SITE, dcsi.CELL, dcsi.ANTENNA_TY, dcsi.AZIMUTH, dcsi.BEAMWIDTH, dcsi.SITE_NAME, dcsi.CITY, dcsi.PROVINCE,
dcsi.ADDRESS, dcsi.X, dcsi.Y, dcsi.LATITUDE, dcsi.LONGITUDE, dcsi.CGI, dcsi.file_date;

