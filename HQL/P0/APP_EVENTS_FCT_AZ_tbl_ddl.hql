CREATE TABLE iptv.app_events_fct_az(                                    
  header struct<`timestamp`:bigint,uuid:string,hostname:string,money:struct<traceId:string,spanId:bigint,parentSpanId:bigint,spanName:string,appName:string,startTime:bigint,spanDuration:bigint,spanSuccess:boolean,notes:map<string,string>>>,                
  device struct<receiverId:string,deviceId:string,deviceSourceId:string,account:string,accountSourceId:string,billingaccountId:string,macAddress:string,ecmMacAddress:string,firmwareVersion:string,deviceType:string,make:string,model:string,partner:string,ipAddress:string,utcOffset:int,postalCode:string,dma:string,isFlex:boolean>,  
  eventtimestamp bigint,     
  appid string,    
  appname string,  
  appaction string,
  eventname string,
  evtsrc string,   
  sessionguid string,   
  eventtype string,
  eventdescription string,   
  pagename string, 
  buttonname string,    
  errorcode string,
  hdp_file_name string, 
  hdp_create_ts string, 
  hdp_update_ts string,
  az_insert_ts timestamp,   
  az_update_ts timestamp,
  exec_run_id STRING
)   
PARTITIONED BY (event_date date)  
STORED AS ORC;  

