INSERT OVERWRITE TABLE IPTV.APP_EVENTS_SUMFCT partition(event_date)
Select
header.`timestamp`               
,header.uuid                                     
,header.hostname                                
,device.receiverid               
,device.deviceid                         
,device.devicesourceid   
,device.account                  
,device.accountsourceid  
,device.billingaccountid  
,device.macaddress               
,device.ecmmacaddress    
,device.firmwareversion  
,device.devicetype               
,device.make                             
,device.model                    
,device.partner                  
,device.ipaddress                
,device.utcoffset                
,device.postalcode               
,eventtimestamp           
,appid                    
,appname                  
,appaction                
,eventname                
,evtsrc                   
,sessionguid              
,eventtype                
,eventdescription         
,pagename                 
,buttonname               
,errorcode                
,hdp_file_name            
,hdp_create_ts            
,hdp_update_ts         
,event_date 
from iptv.app_events_fct;
