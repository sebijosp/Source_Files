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
DROP TABLE IF EXISTS ext_cdrdm.TEMP_PD_SUB;
create table ext_cdrdm.TEMP_PD_SUB STORED AS ORC as  select distinct subscriber_no ,IMSI 
from (select subscriber_no,rank() over(PARTITION BY PD.IMSI  order by PD.esn_seq_no desc) as row_num,IMSI 
from cdrdm.pgwggsn PGW,
ela_v21.physical_device PD 
where PD.eq_type='G' and 
PD.imsi = PGW.served_imsi  and 
unix_timestamp(substr(PD.effective_date,1,10),'yyyy-MM-dd') < unix_timestamp(substr(PGW.record_opening_time,1,8),'yy-MM-dd') ) temp 
where temp.row_num=1;

INSERT INTO TABLE cdrdm.FACT_GPRS_MPG_GGSN_CDR PARTITION (record_opening_date)
SELECT
cast(record_Type_ind as int),
trim(served_IMSI),
trim(ggsn_Address),
trim(charging_ID),
trim(sgsn_Address_1),
trim(sgsn_Address_2),
trim(sgsn_Address_3),
trim(sgsn_Address_4),
trim(sgsn_Address_5),
trim(access_Point_Name_NI),
trim(pdp_Type),
trim(served_PDP_Address),
trim(dynamic_Address_Flag),
trim(qos_Negotiated_1),
cast(data_Volume_GPRS_Uplink_1 as bigint),
cast(data_Volume_GPRS_Downlink_1 as bigint),
trim(change_Condition_1),
trim(change_Time_1),    
if ((change_time_1 is not NULL) and length(trim(change_time_1)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(change_time_1,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL),
trim(user_location_information_1),
if (length(trim(user_location_information_1)) > 0,(CASE WHEN rat_type = 6  THEN  CONCAT(trim(SUBSTR(user_location_information_1,1,3)),'-',trim(SUBSTR(user_location_information_1,4,3)),'-', trim(SUBSTR(user_location_information_1,7,5)),'-',CONV(CONCAT(CONV(TRIM(SUBSTR(user_location_information_1,12,7)),10,16),'0',TRIM(SUBSTR(user_location_information_1,19,5))),16,10)) ELSE  CONCAT(trim(SUBSTR(user_location_information_1,1,3)),'-',trim(SUBSTR(user_location_information_1,4,3)),'-', trim(SUBSTR(user_location_information_1,24,5)),'-',if(length(trim(SUBSTR(user_location_information_1,29,5)))>0 and trim(SUBSTR(user_location_information_1,29,5)) is not NULL,trim(SUBSTR(user_location_information_1,29,5)),trim(SUBSTR(user_location_information_1,19,5)))) END),trim(user_location_information_2)),
trim(qos_Negotiated_2),
cast(data_Volume_GPRS_Uplink_2 as bigint),
cast(data_Volume_GPRS_Downlink_2 as bigint),
change_Condition_2,
trim(change_Time_2),    
if ((change_time_2 is not NULL) and length(trim(change_time_2)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(change_time_2,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL),
trim(user_location_information_2),
if (length(trim(user_location_information_2)) > 0,(CASE WHEN rat_type = 6  THEN  CONCAT(trim(SUBSTR(user_location_information_2,1,3)),'-',trim(SUBSTR(user_location_information_2,4,3)),'-', trim(SUBSTR(user_location_information_2,7,5)),'-',CONV(CONCAT(CONV(TRIM(SUBSTR(user_location_information_2,12,7)),10,16),'0',TRIM(SUBSTR(user_location_information_2,19,5))),16,10)) ELSE  CONCAT(trim(SUBSTR(user_location_information_2,1,3)),'-',trim(SUBSTR(user_location_information_2,4,3)),'-', trim(SUBSTR(user_location_information_2,24,5)),'-',if(length(trim(SUBSTR(user_location_information_2,29,5)))>0 and trim(SUBSTR(user_location_information_2,29,5)) is not NULL,trim(SUBSTR(user_location_information_2,29,5)),trim(SUBSTR(user_location_information_2,19,5)))) END),trim(user_location_information_2)),
trim(qos_Negotiated_3),
cast(data_Volume_GPRS_Uplink_3 as bigint),
cast(data_Volume_GPRS_Downlink_3 as bigint),
change_Condition_3,
trim(change_Time_3),    
if ((change_time_3 is not NULL) and length(trim(change_time_3)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(change_time_3,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL),
trim(user_location_information_3),
if (length(trim(user_location_information_3)) > 0,(CASE WHEN rat_type = 6  THEN  CONCAT(trim(SUBSTR(user_location_information_3,1,3)),'-',trim(SUBSTR(user_location_information_3,4,3)),'-', trim(SUBSTR(user_location_information_3,7,5)),'-',CONV(CONCAT(CONV(TRIM(SUBSTR(user_location_information_3,12,7)),10,16),'0',TRIM(SUBSTR(user_location_information_3,19,5))),16,10)) ELSE  CONCAT(trim(SUBSTR(user_location_information_3,1,3)),'-',trim(SUBSTR(user_location_information_3,4,3)),'-', trim(SUBSTR(user_location_information_3,24,5)),'-',if(length(trim(SUBSTR(user_location_information_3,29,5)))>0 and trim(SUBSTR(user_location_information_3,29,5)) is not NULL,trim(SUBSTR(user_location_information_3,29,5)),trim(SUBSTR(user_location_information_3,19,5)))) END) ,trim(user_location_information_3)),
trim(qos_Negotiated_4),
cast(data_Volume_GPRS_Uplink_4 as bigint),
cast(data_Volume_GPRS_Downlink_4 as bigint),
change_Condition_4,
trim(change_Time_4),    
if ((change_time_4 is not NULL) and length(trim(change_time_4)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(change_time_4,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL),
trim(user_location_information_4),
if (length(trim(user_location_information_4)) > 0,(CASE WHEN rat_type = 6  THEN  CONCAT(trim(SUBSTR(user_location_information_4,1,3)),'-',trim(SUBSTR(user_location_information_4,4,3)),'-', trim(SUBSTR(user_location_information_4,7,5)),'-',CONV(CONCAT(CONV(TRIM(SUBSTR(user_location_information_4,12,7)),10,16),'0',TRIM(SUBSTR(user_location_information_4,19,5))),16,10)) ELSE  CONCAT(trim(SUBSTR(user_location_information_4,1,3)),'-',trim(SUBSTR(user_location_information_4,4,3)),'-', trim(SUBSTR(user_location_information_4,24,5)),'-',if(length(trim(SUBSTR(user_location_information_4,29,5)))>0 and trim(SUBSTR(user_location_information_4,29,5)) is not NULL,trim(SUBSTR(user_location_information_4,29,5)), trim(SUBSTR(user_location_information_4,19,5)))) END) ,trim(user_location_information_4)),
trim(qos_Negotiated_5),
cast(data_Volume_GPRS_Uplink_5 as bigint),
cast(data_Volume_GPRS_Downlink_5 as bigint),
change_Condition_5,
trim(change_Time_5),    
if ((change_time_5 is not NULL) and length(trim(change_time_5)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(change_time_5,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL),
trim(user_location_information_5),
if (length(trim(user_location_information_5)) > 0,(CASE WHEN rat_type = 6  THEN  CONCAT(trim(SUBSTR(user_location_information_5,1,3)),'-',trim(SUBSTR(user_location_information_5 ,4,3)),'-', trim(SUBSTR(user_location_information_5,7,5)),'-',CONV(CONCAT(CONV(TRIM(SUBSTR(user_location_information_5,12,7)),10,16),'0',TRIM(SUBSTR(user_location_information_5,19,5))),16,10)) ELSE  CONCAT(trim(SUBSTR(user_location_information_5 ,1,3)),'-',trim(SUBSTR(user_location_information_5 ,4,3)),'-', trim(SUBSTR(user_location_information_5 ,24,5)),'-',if(length(trim(SUBSTR(user_location_information_5 ,29,5)))>0 and trim(SUBSTR(user_location_information_5 ,29,5)) is not NULL,trim(SUBSTR(user_location_information_5 ,29,5)), trim(SUBSTR(user_location_information_5 ,19,5) )) ) END) ,trim(user_location_information_5)),
record_Opening_Time,
if ((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(record_Opening_Time,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL) as record_opening_time,
cast(duration as int),
cast(cause_For_Rec_Closing as int),
cast(record_Sequence_Number as bigint),
node_ID,
cast(local_Sequence_Number as bigint),
cast(apn_Selection_Mode as int),
served_MSISDN,
cast(substr(trim(served_MSISDN),4,10) as bigint) as subscriber_no,
b.subscriber_no,
charging_Characteristics,
ch_Ch_Selection_Mode,
iMS_signaling_Context,
sgsn_PLMN_Identifier,
served_IMEI_SV,
cast(rAT_Type as int),
mS_Time_Zone,
trim(user_location_information),
if (length(trim(user_location_information)) > 0,(CASE WHEN rat_type = 6  THEN  CONCAT(TRIM(SUBSTR(user_location_information ,1,3)),'-',TRIM(SUBSTR(user_location_information ,4,3)),'-', trim(SUBSTR(user_location_information,7,5)),'-',CONV(CONCAT(CONV(TRIM(SUBSTR(user_location_information,12,7)),10,16),'0',TRIM(SUBSTR(user_location_information,19,5))),16,10)) ELSE  CONCAT(TRIM(SUBSTR(user_location_information ,1,3)),'-',TRIM(SUBSTR(user_location_information ,4,3)),'-', TRIM(SUBSTR(user_location_information ,24,5)),'-',if(length(trim(SUBSTR(user_location_information ,29,5)))>0 and TRIM(SUBSTR(user_location_information ,29,5)) is not NULL,TRIM(SUBSTR(user_location_information ,29,5)), TRIM(SUBSTR(user_location_information ,19,5)) ) ) END ),trim(user_location_information)),
trim(serving_Node_Type_1),
trim(serving_Node_Type_2),
trim(serving_Node_Type_3),
trim(serving_Node_Type_4),
trim(serving_Node_Type_5),
trim(pgw_plmn_identifer),
trim(start_time),
if ((start_time is not NULL) and length(trim(start_time)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(start_time,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL),
trim(stop_time),
if ((start_time is not NULL) and length(trim(stop_time)) > 0 ,FROM_UNIXTIME(UNIX_TIMESTAMP(substr(regexp_replace(stop_time,':','.'),1,18), 'yy-MM-dd HH.mm.ss'), 'yyyy-MM-dd HH:mm:ss'),NULL),
trim(pdn_connection_id),
trim(srvd_PDP_PDN_Addr_Ext),
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
if((record_Opening_Time is not NULL) and length(trim(record_Opening_Time)) > 0, concat('20', substr(record_Opening_Time,1,8)),NULL) as record_opening_date
FROM cdrdm.pgwggsn a left outer join  ext_cdrdm.TEMP_PD_SUB b ON (b.imsi=a.served_imsi) ${hiveconf:WHERE_clause};
