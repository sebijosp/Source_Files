set hive.cli.print.header=true;
SET hive.execution.engine=tez;
select
substr(insert_timestamp,1,10) as insert_timestamp,
local_time_stamp_date,
plmn_tadig_from_tap_file,
serving_bid,
serving_location_desc,
location_area,
cellid,
imsi,
cause_for_term,
sum(if(record_type_ind='MOC' AND total_call_event_duration >0 AND(trim(sms_destination_number) is null or trim(sms_destination_number) = ''),1,0)) as Count_of_MOC_Calls,
sum(if(record_type_ind='MTC' AND total_call_event_duration >0 AND (trim(sms_destination_number) is null or trim(sms_destination_number) = ''),1,0)) as Count_of_MTC_Calls,
sum(if(record_type_ind='MOC' AND total_call_event_duration >0 AND (trim(sms_destination_number) is null or trim(sms_destination_number) = ''),total_call_event_duration,0)) as Sum_of_MOC_Call_Duration,
sum(if(record_type_ind='MTC' AND total_call_event_duration >0 AND (trim(sms_destination_number) is null or trim(sms_destination_number) = ''),total_call_event_duration,0)) as Sum_of_MTC_Call_Duration,
sum(if(record_type_ind='MOC' AND (trim(sms_destination_number) is not null and trim(sms_destination_number) <> ''),1,0)) as Count_of_SMS_Originating,
sum(if((record_type_ind='MTC' AND (trim(sms_destination_number) is not null and trim(sms_destination_number) <> ''))
or (record_type_ind='MTC' AND  total_call_event_duration = 0 AND (trim(sms_destination_number) is  null or trim(sms_destination_number) = ''))
,1,0)) as Count_of_SMS_Terminating,
sum( cast (data_volume_incoming as bigint) + cast (data_volume_outgoing as bigint) ) as Sum_of_Data_Volume
--sum(data_volume_incoming+data_volume_outgoing) as Sum_of_Data_Volume
FROM cdrdm.fact_tap3in_cdr
WHERE substr(insert_timestamp,1,10)='2022-06-28'
and PLMN_TADIG_From_TAP_FILE like 'CAN%'
group by
substr(insert_timestamp,1,10),
local_time_stamp_date,
plmn_tadig_from_tap_file,
serving_bid,
serving_location_desc,
location_area,
cellid,
imsi,
cause_for_term order by cellid;
