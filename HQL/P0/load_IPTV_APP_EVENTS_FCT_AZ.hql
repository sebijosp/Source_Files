!echo ;
!echo Hive : Drop Partitions;


Alter table iptv.app_events_fct_az drop if exists partition (event_date > '1970-01-01');

!echo;
!echo Hive :Insert into the table;

Insert overwrite  table  iptv.app_events_fct_az Partition(event_date)
select a.header, a.device, a.eventtimestamp, a.appid, a.appname, a.appaction, a.eventname, a.evtsrc, a.sessionguid, a.eventtype, a.eventdescription, a.pagename, a.buttonname, a.errorcode, a.hdp_file_name, a.hdp_create_ts, a.hdp_update_ts, 
current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id,a.event_date 
from iptv.app_events_fct as a 
where 
event_date >= '${hiveconf:DeltaPartStart}' and event_date < trunc(current_date, 'MM');

Insert overwrite  table  iptv.app_events_fct_az Partition(event_date)
select a.header, a.device, a.eventtimestamp, a.appid, a.appname, a.appaction, a.eventname, a.evtsrc, a.sessionguid, a.eventtype, a.eventdescription, a.pagename, a.buttonname, a.errorcode, a.hdp_file_name, a.hdp_create_ts, a.hdp_update_ts, 
current_timestamp as Az_insert_ts, current_timestamp as Az_update_ts, 0 as exec_run_id,a.event_date 
from iptv.app_events_fct as a 
where 
event_date >=  date_sub('${hiveconf:DeltaPartStart}', 5) and event_date <  '${hiveconf:DeltaPartStart}' 
and hdp_create_ts >  cast(regexp_replace('${hiveconf:DeltahdpCreatTs}', 'T', ' ') as timestamp);

!echo ;
!echo Hive : Data loaded; 


