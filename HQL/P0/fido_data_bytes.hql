set hive.execution.engine=tez;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.tez.container.size=8192;

with tmp_databytes_sample as
(
select  
subscriber_no_char,
record_opening_date,
record_opening_time,
cast(concat(record_opening_date,' ',record_opening_time,'.000') as timestamp) as databyte_start,
served_imei,
data_volume_uplink_archive,
data_volume_downlink_archive,
event_count,
case when duration < 0 then 0 else duration end as duration,
cell_id,
tracking_area_code,
eutran_cellid,
record_sequence_number
from cdrdm.vw_fact_gprs_cdr
where trim(service_class_group) = 'HHPASS'
and substr(record_opening_date,1,10) between '${hiveconf:start_date}' and '${hiveconf:end_date}' --'2019-04-20' and '2019-04-20'
),
tmp_fido_ctn_lookup as
(
select 
customer_ban, subscriber_no, 
init_activation_date, deactivation_date,
account_type, account_sub_type,
A.pp_acc_type_desc,
A.pp_acc_subtype_desc,
coalesce(deactivation_date,cast('9999-12-31 23:59:59.000' as timestamp)) as outside_date
from ods.subscriber S
join ela_v21.account_type_gg A
on (S.account_type = A.acc_type and S.account_sub_type = A.acc_sub_type)
where franchise_tp = 'F' 
and case when to_date(deactivation_date) < '2017-01-01' then 0 else 1 end = 1
),
tmp_databytes_summary as
(
select
cast(sha2(cast(B.customer_ban as varchar(9)),256) as varchar(64)) as hashed_ban,
cast(sha2(subscriber_no_char,256) as varchar(64)) as hashed_ctn,
B.customer_ban as ban,
subscriber_no_char as ctn,
session_time as trans_dt,
session_time as interaction_start_ts,
case
when duration > 3600
then cast(from_unixtime(cast(unix_timestamp(session_time) as bigint) + 3600) as timestamp)
else cast(from_unixtime(cast(unix_timestamp(session_time) as bigint) + COALESCE(cast(duration as BIGINT), CAST(0 AS BIGINT))) as timestamp)
end as interaction_end_ts,
case when duration > 3600 then 3600 else duration end as duration,
'FDB' as data_source,
'Fido Data Bytes Session' as event_description,
concat(trim(cast(session_mb_used as string)),' MB used') as interaction_outcome,
trim(cast(session_mb_used as string)) as data_mb_used,
'Fido Wireless' as product_group,
'Mobile' as trans_channel_level_1,
'MOBILE APP' as trans_channel_level_2,
B.pp_acc_type_desc as account_type,
B.pp_acc_subtype_desc as account_subtype,
'Products, Services and Ordering' as disposition_reason_cd_1,
'Activate Fido Data Bytes Session' as disposition_reason_cd_3,
row_number() over (partition by subscriber_no_char, session_time order by session_time desc) as row_seq,
to_date(session_time) as partition_dt
from (
select
subscriber_no_char,
session_time,
round(sum(mb_used),2) as session_mb_used,
max(duration) as duration,
max(served_imei) as imei,
count(*) as session_segments
from (
select
D4.*,
case when secs_from_first <= 3600 then first_start when secs_between <= 3600 then prev_session else databyte_start end as session_time 
from (
select
D3.*,
unix_timestamp(D3.databyte_start) - unix_timestamp(D3.prev_session) as secs_between,
unix_timestamp(D3.databyte_start) - unix_timestamp(D3.first_start) as secs_from_first
from (
select
D2.*,
(data_volume_uplink_archive+data_volume_downlink_archive)/1024/1024 as mb_used,
lag(D2.databyte_start) over (partition by D2.subscriber_no_char order by D2.databyte_start) as prev_session,
min(D2.databyte_start) over (partition by D2.subscriber_no_char, D2.record_opening_date) as first_start
from (
select 
D.*
from tmp_databytes_sample D
) D2
) D3
) D4
) D5
group by 
subscriber_no_char,
session_time
) D6  
join tmp_fido_ctn_lookup B
on (D6.subscriber_no_char = B.subscriber_no)
where D6.session_time between B.init_activation_date and B.outside_date
),
final_databytes as
(
select 
hashed_ban,
hashed_ctn,
ban,
ctn,
trans_dt,
interaction_start_ts,
interaction_end_ts,
COALESCE(cast(duration as BIGINT), CAST(0 AS BIGINT)) as duration,
data_source,
event_description,
interaction_outcome,
COALESCE(data_mb_used, 0) as data_mb_used,
product_group,
trans_channel_level_1,
trans_channel_level_2,
account_type,
account_subtype,
disposition_reason_cd_1,
disposition_reason_cd_3,
partition_dt
from tmp_databytes_summary
where row_seq = 1
)
insert overwrite table cdrdm.fido_data_bytes partition(partition_dt) select * from final_databytes