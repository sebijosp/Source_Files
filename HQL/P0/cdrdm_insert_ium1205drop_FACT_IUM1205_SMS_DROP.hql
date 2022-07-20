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

INSERT INTO TABLE cdrdm.FACT_IUM1205_SMS_DROP PARTITION (channel_seizure_date)
SELECT
subscriber_no,
concat(substr (channel_seizure_date,1,4),'-',substr (channel_seizure_date,5,2),'-',substr (channel_seizure_date,7,2),' ',substr (channel_seizure_date,9,2),':',substr (channel_seizure_date,11,2),':',substr (channel_seizure_date,13,2)) as channel_seizure_ts,
message_switch_id,
call_action_code,
unit_esn,
cast(trim(ring_time) as int),
at_call_dur_sec,
call_term_code,
call_to_tn,
orig_cell_trunk_id,
term_cell_trunk_id,
serv_sid,
cast(trim(home_sid) as varchar(5)),
cast(trim(mps_file_number) as int),
toll_type,
market_code,
cast(trim(message_type) as varchar(1)),
call_direction_code,
characteristics,
error_codes,
struct_code,
call_type,
units_uom,
cast(trim(call_data_module_choice_mask) as int),
imsi,
imei,
exchange_identity,
switch_identity,
concat(substr(date_for_start_of_charge,1,4),'-',substr (date_for_start_of_charge,5,2),'-',substr (date_for_start_of_charge,7,2)) as date_for_start_of_charge,
cast(trim(time_for_start_of_charge) as int),
chargeable_duration,
incoming_route,
outgoing_route,
calling_party_number,
called_party_number,
original_called_number,
cast(trim(tm_frm_rgstr_szr_to_strt_chrg) as int),
mobile_station_roaming_nbr,
redirecting_number,
cell_id_for_first_cell,
calling_party_spid,
called_party_spid,
sms_key_word,
provider_name,
in_indicator,
record_type_ind,
call_id,
do_dupcheck,
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
concat(substr (channel_seizure_date,1,4),'-',substr (channel_seizure_date,5,2),'-',substr (channel_seizure_date,7,2)) as channel_seizure_date
FROM cdrdm.ium1205drop a ${hiveconf:WHERE_clause};