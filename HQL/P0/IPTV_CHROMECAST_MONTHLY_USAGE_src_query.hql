!echo Info Hive: Drop temp table;
DROP TABLE IF EXISTS EXT_IPTV.CHROMECAST_MONTHLY_USAGE_TMP;

!echo Info Hive: Creating temp table;
create table EXT_IPTV.CHROMECAST_MONTHLY_USAGE_TMP(
service_account_id      string,
account_source_id    string,
device_name  string,
event_month string)
stored as ORC;

!echo Info Hive: Loading new data in temp table;
insert into table ext_iptv.chromecast_monthly_usage_tmp
SELECT DISTINCT 
  device.serviceaccountid,
  device.accountsourceid,
  device.devicename,
  SUBSTRING(event_date, 1, 7) as event_month
FROM iptv.ip_playback_fct
WHERE device.devicename = 'chromecast'
AND hdp_update_ts > '${DeltaPartStartTs}';


!echo Info Hive: Loading existing data in temp table;
insert into table ext_iptv.chromecast_monthly_usage_tmp
SELECT 
  service_account_id,
  account_source_id,
  device_name,
  event_month
  FROM iptv.chromecast_monthly_usage
WHERE event_month in (select distinct event_month from ext_iptv.chromecast_monthly_usage_tmp);


!echo Info Hive: Loading data to target table !!;
insert overwrite table iptv.chromecast_monthly_usage partition(event_month)
SELECT 
   service_account_id,
   account_source_id,
   device_name,
   hdp_insert_ts,
   hdp_update_ts,
   event_month
FROM (
   SELECT 
      service_account_id,
      account_source_id,
      device_name,
      from_unixtime(unix_timestamp()) hdp_insert_ts,
      from_unixtime(unix_timestamp()) hdp_update_ts,
      event_month,
      ROW_NUMBER() OVER (PARTITION BY service_account_id, account_source_id, device_name,event_month ORDER BY NULL) rownum
   FROM EXT_IPTV.CHROMECAST_MONTHLY_USAGE_TMP
   ) a
WHERE rownum = 1;

!echo Info Hive: Target table load completed !!;
