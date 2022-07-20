SET hive.variable.substitute=true;
SET hive.auto.convert.join=false;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
--SET hive.vectorized.execution.enabled=false;
--SET hive.vectorized.execution.reduce.enabled=false;
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
SET hive.exec.orc.default.buffer.size=65536;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;
--SET tez.task.resource.memory.mb=8192;
--SET tez.am.resource.memory.mb=8192;
set hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=10000000;
--SET mapreduce.map.memory.mb=8192;
--SET mapreduce.reduce.memory.mb=8192;
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx8500m;
set mapreduce.reduce.memory.mb=9000;
set mapreduce.reduce.java.opts=-Xmx8500m;
set mapreduce.task.timeout=3600000;

SET WHERE_clause;

DROP TABLE IF EXISTS ext_cdrdm.tap3ascii_temp1;

CREATE TABLE IF NOT EXISTS ext_cdrdm.tap3ascii_temp1 AS
SELECT
trim(a.record_type_ind) as record_type_ind,
trim(a.imsi) as imsi,
if ((a.msisdn is not NULL and length(a.msisdn) > 0), trim(msisdn), NULL) as msisdn,
if ((a.called_number is not NULL and length(a.called_number) > 0), trim(a.called_number), NULL),
if ((a.calling_number is not NULL and length(a.calling_number) > 0), trim(a.calling_number), NULL),
if ((a.dialled_digits is not NULL and length(a.dialled_digits) > 0), trim(a.dialled_digits), NULL),
if ((a.imei is not NULL and length(a.imei) > 0), trim(a.imei), NULL),
a.tele_service_code,
concat(substr (a.local_time_stamp,1,4),'-',substr (a.local_time_stamp,5,2),'-',substr (a.local_time_stamp,7,2),' ',substr (a.local_time_stamp,9,2),':',substr (a.local_time_stamp,11,2),':',substr (a.local_time_stamp,13,2)) as local_time_stamp,
trim(a.utc_time_offsetcode),
if ((a.location_area is not NULL and length(a.location_area) > 0), trim(a.location_area), NULL),
if ((a.cellid is not NULL and length(a.cellid) > 0), trim(a.cellid), NULL),
if ((a.camel_destination_number is not NULL and length(a.camel_destination_number) > 0), trim(a.camel_destination_number), NULL),
if ((a.camel_service_key is not NULL and length(a.camel_service_key) > 0), trim(a.camel_service_key), NULL),
if ((a.camel_service_level is not NULL and length(a.camel_service_level) > 0), trim(a.camel_service_level), NULL),
if ((a.cause_for_term is not NULL and length(a.cause_for_term) > 0), trim(a.cause_for_term), NULL),
if ((a.pdp_address is not NULL and length(a.pdp_address) > 0), trim(a.pdp_address), NULL),
if ((a.access_point_name_ni is not NULL and length(a.access_point_name_ni) > 0), trim(a.access_point_name_ni), NULL),
cast(a.total_call_event_duration as int),
cast(a.data_volume_incoming as int),
cast(a.data_volume_outgoing as int),
if ((a.call_type_level_1 is not NULL and length(a.call_type_level_1) > 0), trim(a.call_type_level_1), NULL),
if ((a.call_type_level_2 is not NULL and length(a.call_type_level_2) > 0), trim(a.call_type_level_2), NULL),
if ((a.call_type_level_3 is not NULL and length(a.call_type_level_3) > 0), trim(a.call_type_level_3), NULL),
cast(a.tap_decimal_places as int),
cast(a.Number_decimal_place as int),
a.charged_item_1,
a.charge_type_1,
cast(a.charge_1 as int),
cast(a.chargeable_units_1 as int),
cast(a.charged_units_1 as int),
a.charged_item_2,
a.charge_type_2,
cast(a.charge_2 as int),
cast(a.chargeable_units_2 as int),
cast(a.charged_units_2 as int),
a.charged_item_3,
a.charge_type_3,
cast(a.charge_3 as int),
cast(a.chargeable_units_3 as int),
cast(a.charged_units_3 as int),
a.charged_item_4,
a.charge_type_4,
cast(a.charge_4 as int),
cast(a.chargeable_units_4 as int),
cast(a.charged_units_4 as int),
a.charged_item_5,
a.charge_type_5,
cast(a.charge_5 as int),
cast(a.chargeable_units_5 as int),
cast(a.charged_units_5 as int),
a.charged_item_6,
a.charge_type_6,
cast(a.charge_6 as int),
cast(a.chargeable_units_6 as int),
cast(a.charged_units_6 as int),
a.tax_code_1,
cast(a.tax_value_1 as int),
a.tax_code_2,
cast(a.tax_value_2 as int),
a.tax_code_3,
cast(a.tax_value_3 as int),
a.tax_code_4,
cast(a.tax_value_4 as int),
a.tax_code_5,
cast(a.tax_value_5 as int),
a.charging_id,
a.default_call_handling,
a.exchange_rate,
if ((a.rec_entity_code is not NULL and length(a.rec_entity_code) > 0), trim(a.rec_entity_code), NULL),
if ((a.access_point_name_oi is not NULL and length(a.access_point_name_oi) > 0), trim(a.access_point_name_oi), NULL),
a.transparency_indicator,
concat(substr (a.bc_file_available_timestamp,1,4),'-',substr (a.bc_file_available_timestamp,5,2),'-',substr (a.bc_file_available_timestamp,7,2),' ',substr (a.bc_file_available_timestamp,9,2),':',substr (a.bc_file_available_timestamp,11,2),':',substr (a.bc_file_available_timestamp,13,2)) as bc_file_available_timestamp,
concat(substr (a.bc_file_creation_timestamp,1,4),'-',substr (a.bc_file_creation_timestamp,5,2),'-',substr (a.bc_file_creation_timestamp,7,2),' ',substr (a.bc_file_creation_timestamp,9,2),':',substr (a.bc_file_creation_timestamp,11,2),':',substr (a.bc_file_creation_timestamp,13,2)) as bc_file_creation_timestamp,
cast(a.bc_file_sequence_number as int),
a.bc_file_type_indicator,
a.bc_recipient,
a.bc_sender,
concat(substr (a.bc_trans_cutoff_timestamp,1,4),'-',substr (a.bc_trans_cutoff_timestamp,5,2),'-',substr (a.bc_trans_cutoff_timestamp,7,2),' ',substr (a.bc_trans_cutoff_timestamp,9,2),':',substr (a.bc_trans_cutoff_timestamp,11,2),':',substr (a.bc_trans_cutoff_timestamp,13,2)) as bc_trans_cutoff_timestamp,
a.home_bid,
a.home_location_description,
a.pdp_contxt_start_timestamp,
a.serving_bid,
a.serving_location_desc,
a.serving_network,
a.sms_destination_number,
UPPER(a.switch_name) as PLMN_TADIG_From_TAP_FILE,
if(length(a.msisdn) = 0 or a.msisdn = NULL,-1, if(length(a.msisdn) > 0, if(substr(a.msisdn,1,1)='1', substr(a.msisdn,2,length(a.msisdn)-1), a.msisdn),0)) as subscriber_no,
substr(a.imsi,1,6) as PLMN_From_IMSI,
trim(b.subscriber_no) as PD__SUBSCRIBER_NO,
c.ban as BAN,
c.account_sub_type as ACCOUNT_SUB_TYPE,
trim(c.company_code) as COMPANY_CODE,
c.sub_type as SUBTYPE,
trim(c.tr_ba_company_code) as TR_BA_COMPANY_CODE,
trim(c.tr_ba_company_name) as TR_BA_COMPANY_NAME,
c.tr_ba_account_type as TR_BA_ACCOUNT_TYPE,
if (d.sp_name IS NULL and find_in_set(substr(trim(a.imsi),1,6), '302720,302820') > 0,'ROGERS',if(d.sp_name IS NULL and find_in_set(substr(trim(a.imsi),1,6),'302370') > 0,'FIDO', d.sp_name)) as BRAND_BIMSI__SP_NAME,
trim(c.franchise_cd) as FRANCHISE_CD,
ROW_NUMBER() OVER( ORDER BY a.IMSI, a.record_type_ind, a.local_time_stamp, a.called_number, a.calling_number) as TAP_SNO,
ROW_NUMBER() OVER( PARTITION BY b.IMSI,a.local_time_stamp, ROW_NUMBER() OVER( ORDER BY a.IMSI, a.record_type_ind, a.local_time_stamp, a.called_number, a.calling_number) ORDER BY b.IMSI, b.ESN_SEQ_NO DESC, from_unixtime(unix_timestamp()) DESC, b.EFFECTIVE_DATE DESC ) as max_seq_RN,
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
FROM cdrdm.tap3ascii a LEFT JOIN ela_v21.physical_device b ON (trim(a.imsi) = trim(b.imsi) AND b.eq_type = 'G' AND b.effective_date < to_date(from_unixtime(unix_timestamp())) AND b.expiration_date IS NULL) LEFT JOIN cdrdm.dim_MPS_CUST_1 c ON (b.subscriber_no = c.subscriber_no AND c.mps_cust_seq_no = 1 AND c.product_type = 'C') LEFT JOIN ela_v21.wholesale_service_all_imsi d ON (trim(cast(d.imsi as VARCHAR(20))) = trim(a.imsi)) ${hiveconf:WHERE_clause};

INSERT INTO TABLE cdrdm.FACT_TAP3IN_CDR PARTITION (local_time_stamp_date)
SELECT *,
from_unixtime(unix_timestamp()),
substr(local_time_stamp,1,10) as local_time_stamp_date
FROM ext_cdrdm.tap3ascii_temp1 WHERE max_seq_RN = 1;

INSERT INTO TABLE cdrdm.FACT_TAP3IN_CDR_STG 
SELECT *,
from_unixtime(unix_timestamp()),
substr(local_time_stamp,1,10) as local_time_stamp_date
FROM ext_cdrdm.tap3ascii_temp1 WHERE max_seq_RN = 1;

DROP TABLE IF EXISTS ext_cdrdm.tap3ascii_temp1;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--FROM cdrdm.test_tap3ascii a LEFT JOIN ela_v21.physical_device b ON ("302370602092118" = b.imsi AND b.eq_type = 'G' AND '2014-09-30' < to_date(from_unixtime(unix_timestamp()))) LEFT JOIN cdrdm.dim_MPS_CUST_1 c ON ('1eea024ce84cef12c0d41f80792627' = c.subscriber_no AND c.mps_cust_seq_no = 1 AND c.product_type = 'C') LEFT JOIN cdrdm.wholesale_service_all_imsi d ON (trim(d.imsi) = "5598265bbd57be4fc48752235ca9f51dd7a24106b253307c83");

--ROW_NUMBER() OVER( ORDER BY a.IMSI, a.record_type_ind, a.local_time_stamp, a.called_number, a.calling_number) as TAP_SNO
--MAX(b.ESN_SEQ_NO) OVER( PARTITION BY b.IMSI,from_unixtime(unix_timestamp()) ORDER BY b.IMSI, from_unixtime(unix_timestamp()) DESC, b.EFFECTIVE_DATE DESC) as max_seq --used for what?
--ROW_NUMBER() OVER( PARTITION BY b.IMSI,from_unixtime(unix_timestamp()),TAP_SNO ORDER BY b.IMSI, b.ESN_SEQ_NO DESC, from_unixtime(unix_timestamp()) DESC, b.EFFECTIVE_DATE DESC ) as max_seq_RN
