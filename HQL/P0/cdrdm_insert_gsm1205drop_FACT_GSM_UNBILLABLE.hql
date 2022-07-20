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

INSERT INTO TABLE cdrdm.FACT_GSM_UNBILLABLE PARTITION (channel_seizure_date)
SELECT
trim(SUBSCRIBER_NO) as subscriber_no,
SUBSCRIBER_NO as subscriber_no_char,
if((CHANNEL_SEIZURE_DATE is not NULL) and length(trim(CHANNEL_SEIZURE_DATE)) > 0, concat(SUBSTR(CHANNEL_SEIZURE_DATE,9,2),':',SUBSTR(CHANNEL_SEIZURE_DATE,11,2),':',SUBSTR(CHANNEL_SEIZURE_DATE,13,2)), NULL) as channel_seizure_time,
CALL_ACTION_CODE as call_action_code,
trim(UNIT_ESN) as unit_esn,
case when trim(RING_TIME) = '' then NULL else cast(trim(RING_TIME) as int) end as ring_time,
case when trim(AT_CALL_DUR_SEC) = '' then NULL else cast(trim(AT_CALL_DUR_SEC) as int) end as at_call_dur_sec,
CALL_TERM_CODE as call_term_code,
CALL_TO_TN as call_to_tn,
SERV_SID as serv_sid,
HOME_SID as home_sid,
MPS_FILE_NUMBER as mps_file_number,
TOLL_TYPE as toll_type,
MARKET_CODE as market_code,
MESSAGE_TYPE as message_type,
CALL_DIRECTION_CODE as call_direction_code,
trim(CHARACTERISTICS_1) as characteristics_1,
substr(trim(ERROR_CODES),6) as additional_error_codes,
STRUCT_CODE as struct_code,
CALL_TYPE as call_type,
UNITS_UOM as units_uom,
CALL_DATA_MODULE_CHOICE_MASK as call_data_module_choice_mask,
IMSI as imsi,
IMEI as imei,
EXCHANGE_IDENTITY as exchange_identity,
SWITCH_IDENTITY as switch_identity,
case when DATE_FOR_START_OF_CHARGE = '' OR length(trim(DATE_FOR_START_OF_CHARGE)) <> 8 then null else concat(substr(trim(DATE_FOR_START_OF_CHARGE),1,4),'-',substr(trim(DATE_FOR_START_OF_CHARGE),5,2),'-', substr(trim(DATE_FOR_START_OF_CHARGE),7,2))end as date_for_start_of_charge,
case when TIME_FOR_START_OF_CHARGE = '' then null else concat(substr(trim(TIME_FOR_START_OF_CHARGE) ,1,2),':',substr(trim(TIME_FOR_START_OF_CHARGE) ,3,2),':',substr(trim(TIME_FOR_START_OF_CHARGE) ,5,2)) end as time_for_start_of_charge,
case when CHARGEABLE_DURATION = ' ' then null else (cast(substr(trim(CHARGEABLE_DURATION),1,2) as int) * 3600) + (cast(substr(trim(CHARGEABLE_DURATION),3,2) as int) * 60) + (cast(substr(trim(CHARGEABLE_DURATION),5,2) as int) ) end as chargeable_duration,
trim(CALLING_PARTY_NUMBER) as calling_party_number,
trim(CALLED_PARTY_NUMBER) as called_party_number,
trim(ORIGINAL_CALLED_NUMBER) as original_called_number,
case when REG_SEIZURE_CHARGING_START = '' then null else cast(trim(REG_SEIZURE_CHARGING_START) as int) end as reg_seizure_charging_start,
trim(MOBILE_STATION_ROAMING_NUMBER) as mobile_station_roaming_number,
trim(REDIRECTING_NUMBER) as redirecting_number,
MESSAGE_SWITCH_ID as switch_id,
INCOMING_ROUTE as incoming_route_id,
OUTGOING_ROUTE as outgoing_route_id,
CELL_ID_FOR_FIRST_CELL as first_cell_id,
NULL, --calling_reporting_centre_id
NULL, --called_reporting_centre_id
cast(substr(trim(ERROR_CODES),1,4) as char(4)) as unbillable_reason_codes,
NULL, --significant_number_key
NULL, --format_code
SUBSCRIPTION_TYPE as subscription_type,
SRVCC_INDICATOR as srvcc_indicator,
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
if((CHANNEL_SEIZURE_DATE is not NULL) and length(trim(CHANNEL_SEIZURE_DATE)) > 0, concat(substr(CHANNEL_SEIZURE_DATE,1,4),'-',substr(CHANNEL_SEIZURE_DATE,5,2),'-',substr(CHANNEL_SEIZURE_DATE,7,2)),NULL) as channel_seizure_date
FROM cdrdm.gsm1205drop a ${hiveconf:WHERE_clause};