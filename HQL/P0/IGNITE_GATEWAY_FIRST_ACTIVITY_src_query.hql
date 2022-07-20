!echo ;
!echo INFO Hive: Dropping temp table EXT_IPTV.HOME_CONNECTIVITY_AGG_TMP if exists;

drop table if exists EXT_IPTV.HOME_CONNECTIVITY_AGG_TMP;

!echo ;
!echo INFO Hive: Creating temp table EXT_IPTV.HOME_CONNECTIVITY_AGG_TMP;

CREATE TABLE  EXT_IPTV.HOME_CONNECTIVITY_AGG_TMP(
DeviceID        String,
POD_Availability_tmp  Int,
POD_Crash_tmp  Int,
WIFI_QTN_ERROR_tmp  Int,
WIFI_ERROR_REBOOT_tmp  Int,
DOCSIS_Connectivity_temp        Int,
IP_Connectivity_temp    Int,
Mesh_Connectivity_temp  Int,
MoCA_Connectivity_temp  Int,
Network_Connectivity_temp       Int,
Process_Self_Heal_temp  Int,
Process_System_temp     Int,
Unscheduled_Reboot_Self_Heal_temp       Int,
Unscheduled_Reboot_System_temp  Int,
WiFi_Connectivity_temp  Int
)PARTITIONED BY (Event_Date  Date)
STORED AS ORC;

!echo ;
!echo INFO Hive: Temp table created;

!echo ;
!echo INFO Hive: Loading data into temp table EXT_IPTV.HOME_CONNECTIVITY_AGG_TMP;

insert overwrite table EXT_IPTV.HOME_CONNECTIVITY_AGG_TMP PARTITION(Event_Date)
Select t2.devicesourceid as Deviceid,
case when t2.category= 'POD Availability' then 1 else 0 end POD_Availability_temp,
case when t2.category= 'POD Crashes' then 1 else 0 end POD_Crash_temp,
case when t2.category= 'QTN Chip Error' then 1 else 0 end WIFI_QTN_ERROR_temp,
case when t2.category= 'Wifi Reboot due to error' then 1 else 0 end WIFI_ERROR_REBOOT_temp,
case when t2.category= 'DOCSIS Connectivity' then 1 else 0 end DOCSIS_Connectivity_temp,
case when t2.category= 'IP Connectivity' then 1 else 0 end IP_Connectivity_temp,
case when t2.category= 'Mesh Connectivity' then 1 else 0 end Mesh_Connectivity_temp,
case when t2.category= 'MoCA Connectivity' then 1 else 0 end MoCA_Connectivity_temp,
case when t2.category= 'Network Connectivity' then 1 else 0 end Network_Connectivity_temp,
case when t2.category= 'Process Self-Heal' then 1 else 0 end Process_Self_Heal_temp,
case when t2.category= 'Process System' then 1 else 0 end Process_System_temp,
case when t2.category= 'Unscheduled Reboot Self-Heal' then 1 else 0 end Unscheduled_Reboot_Self_Heal_temp,
case when t2.category= 'Unscheduled Reboot System' then 1 else 0 end Unscheduled_Reboot_System_temp,
case when t2.category= 'WiFi Connectivity' then 1 else 0 end WiFi_Connectivity_temp,
t2.event_date as event_date
from
(select distinct t1.devicesourceid,
t1.event_date,t1.eventcode,
m.marker,m.category
from (
select device.devicesourceid,
event_date,
event.eventcode,
event.eventcount from iptv.rdkb_fct
LATERAL VIEW explode(rdkbevents) ev as event
where event_date >= '${hiveconf:start_date}' and event_date <= '${hiveconf:end_date}'
) t1
inner join IPTV.HOMEINTERNET_COVERAGE_MAPPING m
on m.marker = t1.eventcode
and m.type = 'event'
and m.source = 'RDK-B') t2;

!echo ;
!echo INFO Hive: Temp table load completed;

!echo ;
!echo INFO Hive: Dropping partitions >='${hiveconf:start_date}' and <='${hiveconf:end_date}' from IPTV.HOME_CONNECTIVITY_AGG;

alter table IPTV.HOME_CONNECTIVITY_AGG drop if exists partition (event_date>="${hiveconf:start_date}",event_date<="${hiveconf:end_date}");

!echo INFO Hive: Partitions for >='${hiveconf:start_date}' and <='${hiveconf:end_date}' dropped..;
!echo ;
!echo INFO Hive: Loading data into main table IPTV.HOME_CONNECTIVITY_AGG;

insert into table IPTV.HOME_CONNECTIVITY_AGG PARTITION(Event_Date)
Select t3.Deviceid,
t3.POD_Availability,
t3.POD_Crash,
t3.WIFI_QTN_ERROR,
t3.WIFI_ERROR_REBOOT,
t3.DOCSIS_Connectivity,
t3.IP_Connectivity,
t3.Mesh_Connectivity,
t3.MoCA_Connectivity,
t3.Network_Connectivity,
t3.Process_Self_Heal,
t3.Process_System,
t3.Unscheduled_Reboot_Self_Heal,
t3.Unscheduled_Reboot_System,
t3.WiFi_Connectivity,
from_unixtime(unix_timestamp()) as hdp_create_ts,
from_unixtime(unix_timestamp()) as hdp_update_ts,
t3.event_date
from
(select deviceid,event_date,
case when sum(POD_Availability_tmp) > 0 then 1 else 0 end POD_Availability,
case when sum(POD_Crash_tmp) > 0 then 1 else 0 end POD_Crash,
case when sum(WIFI_QTN_ERROR_tmp) > 0 then 1 else 0 end WIFI_QTN_ERROR,
case when sum(WIFI_ERROR_REBOOT_tmp) > 0 then 1 else 0 end WIFI_ERROR_REBOOT,
case when sum(DOCSIS_Connectivity_temp) > 0 then 1 else 0 end DOCSIS_Connectivity,
case when sum(IP_Connectivity_temp) > 0 then 1 else 0 end IP_Connectivity,
case when sum(Mesh_Connectivity_temp) > 0 then 1 else 0 end Mesh_Connectivity,
case when sum(MoCA_Connectivity_temp) > 0 then 1 else 0 end MoCA_Connectivity,
case when sum(Network_Connectivity_temp) > 0 then 1 else 0 end Network_Connectivity,
case when sum(Process_Self_Heal_temp) > 0 then 1 else 0 end Process_Self_Heal,
case when sum(Process_System_temp) > 0 then 1 else 0 end Process_System,
case when sum(Unscheduled_Reboot_Self_Heal_temp) > 0 then 1 else 0 end Unscheduled_Reboot_Self_Heal,
case when sum(Unscheduled_Reboot_System_temp) > 0 then 1 else 0 end Unscheduled_Reboot_System,
case when sum(WiFi_Connectivity_temp) > 0 then 1 else 0 end WiFi_Connectivity
from EXT_IPTV.HOME_CONNECTIVITY_AGG_TMP
GROUP BY deviceid,event_date) t3
where t3.deviceid is not null;

!echo ;
!echo INFO Hive: IPTV.HOME_CONNECTIVITY_AGG load completed;
