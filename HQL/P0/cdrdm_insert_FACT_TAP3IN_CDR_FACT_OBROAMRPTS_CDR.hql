--[Version History]
--0.1 - jayakumar.kuppuswamy - 3/26/2018 - fact_obroamrpts_cdr

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=orc;
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
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

use cdrdm;
INSERT OVERWRITE TABLE fact_obroamrpts_cdr PARTITION(cday, service_type, call_distance_type)
SELECT  
  callday,
  callmonth,
  callstarttime,
  callendtime,
  brand,
  imsi,
  imei,
  msisdn,
  vplmntadigcode,
  visitedcountry,
  vplmnname,
  mccmnc,
  calltype,
  service,
  calledcountry,
  countrycode,
  callednumber,
  dialednumber,
  calldurationsecs,
  calldurationmins,
  datavolume,
  call_type_level_2,
  access_point_name_ni,
  numberofevents,
  servingcellid,
  servinglac,
  insertdate,
  inserttime,
  file_name,
  record_type,
  family_name,
  version_id,
  file_time,
  file_id,
  switch_name,
  NULL as callcorrected,
  cday,
  service_type,
  call_distance_type
from
(
select
  substr(local_time_stamp,1,10) as CallDay,
  date_format(substr(local_time_stamp,1,10),'MMMMM') as CallMonth,  
  substr(local_time_stamp,12,5) as CallStartTime,
  substr(from_unixtime(unix_timestamp(local_time_stamp)+NVL(total_call_event_duration,0)),12,5) as CallEndTime,
  brand_bimsi as Brand,
  imsi as IMSI,
  imei as IMEI,
  msisdn as MSISDN,
  pll.plmn_tadig as VPLMNTADIGCode,
  pll.country_name as VisitedCountry,
  pll.plmn_name as VPLMNName,
  pll.plmn_code as MCCMNC,
  
  case
    when (tele_service_code = '11' OR tele_service_code = '17') AND record_type_ind = 'MOC'
    then 'MOC VOICE'
    when (tele_service_code = '11' OR tele_service_code = '17') AND record_type_ind = 'MTC'
    then 'MTC VOICE'
    when (tele_service_code = '22')
    then 'SMSMO'
    when (tele_service_code = '21')
    then 'SMSMT'
    when (tele_service_code = '12')
    then 'EMERGENCY'
    when (tele_service_code = '' OR tele_service_code is null) AND record_type_ind = 'GPRS'
    then 'DATA'
    else '?'
  end as CallType,
  record_type_ind as Service,
  
  substr(fa.bc_recipient,0,3) as CalledCountry,
  NVL(pll.plmn_country_code,'') as CountryCode,
  cast(called_number as bigint) as CalledNumber,
  dialled_digits as DialedNumber,
  sum(total_call_event_duration) as CallDurationSecs,
  round(sum(total_call_event_duration/60),1) as CallDurationMins,
  sum(data_volume_incoming + data_volume_outgoing) as datavolume,
  call_type_level_2 as call_type_level_2,
  access_point_name_ni as access_point_name_ni,
  count(imsi) as numberofevents,
  conv(cellid,16,10) as ServingCellID,
  conv(location_area,16,10) as ServingLAC,
  cast(substr(local_time_stamp,1,10) as date) as insertdate,
  cast(local_time_stamp as timestamp) as inserttime,
  file_name as file_name,
  record_type as record_type,
  family_name as family_name,
  version_id as version_id,
  file_time as file_time,
  file_id as file_id,
  switch_name as switch_name,
  cast(regexp_replace(substr(local_time_stamp,1,10),'-','') as int) as cday,
  '3G' as service_type,
  'ROAMING' as call_distance_type
  from cdrdm.fact_tap3in_cdr_stg fa 
  left outer join (
    select * from (
      select pl.*,row_number() over(partition by pl.plmn_tadig order by pl.effective_date desc
      ) as rown  from ela_v21.plmn_settlement_gg pl) as pll 
      where pll.rown=1)  pll 
      on (fa.bc_sender=pll.plmn_tadig)
  group by
  substr(local_time_stamp,1,10),
  date_format(substr(local_time_stamp,1,10),'MMMMM'),  
  substr(local_time_stamp,12,5),
  substr(from_unixtime(unix_timestamp(local_time_stamp)+NVL(total_call_event_duration,0)),12,5),
  brand_bimsi,
  imsi,
  imei,
  msisdn,
  pll.plmn_tadig,
  pll.country_name,
  pll.plmn_name,
  pll.plmn_code,
  case
    when (tele_service_code = '11' OR tele_service_code = '17') AND record_type_ind = 'MOC'
    then 'MOC VOICE'
    when (tele_service_code = '11' OR tele_service_code = '17') AND record_type_ind = 'MTC'
    then 'MTC VOICE'
    when (tele_service_code = '22')
    then 'SMSMO'
    when (tele_service_code = '21')
    then 'SMSMT'
    when (tele_service_code = '12')
    then 'EMERGENCY'
    when (tele_service_code = '' OR tele_service_code is null) AND record_type_ind = 'GPRS'
    then 'DATA'
    else '?'
  end,
  record_type_ind,
  substr(fa.bc_recipient,0,3),
  NVL(pll.plmn_country_code,''),
  cast(called_number as bigint),
  dialled_digits,
  call_type_level_2,
  access_point_name_ni,
  conv(cellid,16,10),
  conv(location_area,16,10),
  cast(substr(local_time_stamp,1,10) as date),
  cast(local_time_stamp as timestamp),
  file_name,
  record_type,
  family_name,
  version_id,
  file_time,
  file_id,
  switch_name,
  cast(regexp_replace(substr(local_time_stamp,1,10),'-','') as int),
  '3G',
  'ROAMING'
) 3g_roaming;

truncate table cdrdm.FACT_TAP3IN_CDR_STG;
