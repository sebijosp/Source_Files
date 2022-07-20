insert into cdrdm.fms_ur_sms partition(tm_end_date) select a.*,substr(a.tm_end_time,1,10) as tm_end_date from ${hiveconf:tblname} a; 

