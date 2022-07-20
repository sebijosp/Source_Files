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

CREATE VIEW IF NOT EXISTS cdrdm.vLesGPRSInfo AS
SELECT 
a.subscriber_no,
--a.subscriber_no_char,
a.record_sequence_number,
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
b.file_date
FROM cdrdm.Fact_gprs_cdr_ext_Tbay a LEFT OUTER JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) CGI_CELL FROM cdrdm.DIM_CELL_SITE_INFO) b ON a.cid = TRIM(b.CGI_HEX)
WHERE (CGI_CELL = 1 OR CGI_CELL IS NULL)
GROUP BY a.subscriber_no, a.served_imsi, a.record_opening_date, a.record_opening_time, a.location_area_code, a.access_point_name, a.switch_name, a.destination_url, a.spid,
a.service_class_group, a.content_delivered, a.event_protocol_type, a.wireless_generation, a.domain_1, a.cdr_type_ind, a.served_imei, a.sgsn_address,
a.served_pdp_address, a.plmn_id, a.cell_id, a.served_msisdn, a.record_sequence_number, a.gprs_choice_mask_archive, a.routing_area, a.charging_id, a.customer_type,
a.tracking_area_code, a.eutran_cellid, a.cid, b.file_type, b.SITE, b.CELL, b.ANTENNA_TY, b.AZIMUTH, b.BEAMWIDTH, b.SITE_NAME, b.CITY, b.PROVINCE,
b.ADDRESS, b.X, b.Y, b.LATITUDE, b.LONGITUDE, b.CGI, b.file_date, a.rec_opening_dt_offset;
