

--************************************************************************************************
--************************************************************************************************
--************************************************************************************************
--** Project: DIM_CELL_SITE_INFO FIX
--** Filename: cdrdm_DIM_CELL_SITE_INFO_GSM.hql
--** Created: October 14, 2016
--** Author: Lalit Summan
--** Version: 0.1
--** THIS CODE CREATES A TEMPORARY DIM_CELL_SITE_NEW_LOAD TABLE TO COMPARE NEW NEW LOADS AGAINST 
--** EXISTING DIM_CELL_SITE_INFO HISTORICAL TABLE 
--** THIS CODE LOADS 3 DIM_CELL_SITE_INFO FILES (GSM, LTE, UTMS) INTO TEMPORARY TABLES AND CREATES 
--** 2 NEW COLUMNS PER FILE.
--** THE NEW COMPARISON LOGIC COMPARES HISTORICAL TABLES AGAINST THE NEW LOADS. THERE ARE 3 CASES CONSIDERED.
--** 
--************************************************************************************************
--************************************************************************************************
--************************************************************************************************




--************************************************************************************************
--************************************************************************************************
--**HIVE PARAMETERS
--************************************************************************************************
--************************************************************************************************


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

SET last_proc_data_dt;
SET yesterday_dt;



MSCK REPAIR TABLE ext_cdrdm.dim_cell_site_info_${hiveconf:site_names};

--************************************************************************************************
--************************************************************************************************
--**LOADS GSM NEW DATA INTO A TEMP TABLE
--************************************************************************************************
--************************************************************************************************

drop table if exists cdrdm.dim_cell_site_info_${hiveconf:site_names}_temp1;
CREATE TABLE IF NOT EXISTS cdrdm.dim_cell_site_info_${hiveconf:site_names}_temp1 AS
SELECT
*,
--CONCAT(substring(file_date,1,4),'-',substring(file_date,5,2),'-',substring(file_date,7,2)) as file_date_dash,
LOCATE('-', trim(cgi)) as first_occurence,
LOCATE('-', trim(cgi), cast(LOCATE('-',trim(cgi)) as int)+1) as second_occurrence,
length(cgi) - LOCATE('-', reverse(trim(cgi))) + 1 as last_occurrence
FROM ext_cdrdm.dim_cell_site_info_${hiveconf:site_names} where file_date= concat(substr(${hiveconf:file_dates},1,4), '-', substr(${hiveconf:file_dates},5,2),'-',substr(${hiveconf:file_dates},7,2));

--ALTER TABLE ext_cdrdm.dim_cell_site_info_${hiveconf:site_names} DROP PARTITION (file_date= ${hiveconf:file_dates});
-- WHERE (file_date > ${hiveconf:last_proc_data_dt} AND file_date <= ${hiveconf:yesterday_dt});
--WHERE (file_date > ${hiveconf:last_proc_data_dt} AND file_date <= ${hiveconf:yesterday_dt});


--************************************************************************************************
--************************************************************************************************
--**INSERTS A NEW CGI INTO HISTORICAL TABLE FOR THE LATEST GSM RECORD WITH THE LEAST NUMBER OF NULLS.
--************************************************************************************************
--************************************************************************************************

INSERT INTO TABLE cdrdm.historical_dim_cell_site_info 
SELECT       
concat('RogersCellSiteDatabase','GSM','_',substr(a.file_date,1,4), substr(a.file_date,6,2), substr(a.file_date,9,2),'.csv') as file_name,
trim(a.site),
trim(a.cell),
NULL, --nodeb,
trim(a.cgi),
CONCAT(SUBSTR(trim(a.cgi),1,LOCATE('-',trim(a.cgi))-1),
CONCAT(SUBSTR(trim(a.cgi),LOCATE('-',trim(a.cgi))+1,(LOCATE('-',trim(a.cgi),second_occurrence) - LOCATE('-',trim(a.cgi)))-1),
CONCAT(CONV(SUBSTR(trim(a.cgi),LOCATE('-',trim(a.cgi),second_occurrence - 1)+1,(LOCATE('-',trim(a.cgi),second_occurrence + 1) - LOCATE('-',trim(a.cgi),second_occurrence -1))-1),10,16),CONV(SUBSTR(trim(a.cgi),LOCATE('-',trim(a.cgi),last_occurrence -1)+1),10,16)))) as CGI_HEX,
trim(a.site_name),
--trim(a.original_i_s_date) as original_i_s_date,
trim(a.original_i_s_date) as original_i_s_date,
--trim(a.antenna_type) as antenna_type,
trim(a.antenna_type) as antenna_type,
trim(a.azimuth),
trim(a.beamwidth),
trim(a.x),
trim(a.y),
trim(a.longitude),
trim(a.latitude),
trim(a.address),
trim(a.city),
trim(a.province),
NULL, --ARFCNDL
trim(a.BCCHNO),
--trim(a.locationco) as LOCATIONCO,
trim(a.locationcode) as locationcode,
trim(a.bsic),
NULL, --primaryscramblingcode
'GSM' as file_type, --FILE_TYPE
to_date(from_unixtime(unix_timestamp())), --INSERT_TIMESTAMP
a.file_date 
from 
(
 select * from  
  (
  select *, ROW_NUMBER() OVER (PARTITION BY tmp.cgi ORDER BY tmp.row_null_cnt) as row_cnt FROM
    (
    SELECT *, if(IF(site IS NULL,1,0) + IF(cell IS NULL,1,0) +IF(cgi IS NULL,1,0)+IF(site_name IS NULL,1,0)+IF(original_i_s_date IS NULL,1,0)+IF(antenna_type IS NULL,1,0)+IF(azimuth IS NULL,1,0)+IF(beamwidth IS NULL,1,0)+IF(x IS NULL,1,0)+IF(y IS NULL,1,0)+IF(longitude IS NULL,1,0)+IF(latitude IS NULL,1,0)+IF(address IS NULL,1,0)+IF(city IS NULL,1,0)+IF(province IS NULL,1,0)+IF(locationcode IS NULL,1,0)+IF(bsic IS NULL,1,0)+IF(BCCHNO IS NULL,1,0)+IF(file_date IS NULL,1,0) = NULL, 0, (IF(site IS NULL,1,0) + IF(cell IS NULL,1,0) + IF(cgi IS NULL,1,0)+IF(site_name IS NULL,1,0)+IF(original_i_s_date IS NULL,1,0)+IF(antenna_type IS NULL,1,0)+IF(azimuth IS NULL,1,0)+IF(beamwidth IS NULL,1,0)+IF(x IS NULL,1,0)+IF(y IS NULL,1,0)+IF(longitude IS NULL,1,0)+IF(latitude IS NULL,1,0)+IF(address IS NULL,1,0)+IF(city IS NULL,1,0)+IF(province IS NULL,1,0)+IF(locationcode IS NULL,1,0)+IF(bsic IS NULL,1,0)+IF(BCCHNO IS NULL,1,0)+IF(file_date IS NULL,1,0))) as row_null_cnt FROM cdrdm.dim_cell_site_info_GSM_temp1
    ) as tmp 
   ) as tmp2 where tmp2.row_cnt = 1
) as a
LEFT JOIN   (select cgi, count(*) from cdrdm.historical_dim_cell_site_info group by cgi) as b 
on          TRIM(UPPER(a.cgi)) = TRIM(UPPER(b.cgi))
WHERE       trim(b.cgi) IS NULL;


--************************************************************************************************
--************************************************************************************************
--**INSERTS A CHANGED COLUMNS FOR EXISITING CGI INTO HISTORICAL TABLE FOR THE LATEST GSM RECORD WITH THE LEAST NUMBER OF NULLS.
--************************************************************************************************
--************************************************************************************************

INSERT INTO TABLE cdrdm.historical_dim_cell_site_info 
SELECT       
concat('RogersCellSiteDatabase','GSM','_',substr(a.file_date,1,4), substr(a.file_date,6,2), substr(a.file_date,9,2),'.csv') as file_name,
trim(a.site),
trim(a.cell),
NULL, --nodeb,
trim(a.cgi),
CONCAT(SUBSTR(trim(a.cgi),1,LOCATE('-',trim(a.cgi))-1),
CONCAT(SUBSTR(trim(a.cgi),LOCATE('-',trim(a.cgi))+1,(LOCATE('-',trim(a.cgi),second_occurrence) - LOCATE('-',trim(a.cgi)))-1),
CONCAT(CONV(SUBSTR(trim(a.cgi),LOCATE('-',trim(a.cgi),second_occurrence - 1)+1,(LOCATE('-',trim(a.cgi),second_occurrence + 1) - LOCATE('-',trim(a.cgi),second_occurrence -1))-1),10,16),CONV(SUBSTR(trim(a.cgi),LOCATE('-',trim(a.cgi),last_occurrence -1)+1),10,16)))) as CGI_HEX,
trim(a.site_name),
--trim(a.original_i_s_date) as original_i_s_date,
trim(a.original_i_s_date) as original_i_s_date,
--trim(a.antenna_type) as antenna_type,
trim(a.antenna_type) as antenna_type,
trim(a.azimuth),
trim(a.beamwidth),
trim(a.x),
trim(a.y),
trim(a.longitude),
trim(a.latitude),
trim(a.address),
trim(a.city),
trim(a.province),
NULL, --ARFCNDL
trim(a.BCCHNO),
--trim(a.locationco) as LOCATIONCO,
trim(a.locationcode) as locationcode,
trim(a.bsic),
NULL, --primaryscramblingcode
'GSM' as file_type, --FILE_TYPE
to_date(from_unixtime(unix_timestamp())), --INSERT_TIMESTAMP
a.file_date 
from 
(
 select * from  
  (
  select *, ROW_NUMBER() OVER (PARTITION BY tmp.cgi ORDER BY tmp.row_null_cnt) as row_cnt FROM
    (
    SELECT *, if(IF(site IS NULL,1,0) + IF(cell IS NULL,1,0) +IF(cgi IS NULL,1,0)+IF(site_name IS NULL,1,0)+IF(original_i_s_date IS NULL,1,0)+IF(antenna_type IS NULL,1,0)+IF(azimuth IS NULL,1,0)+IF(beamwidth IS NULL,1,0)+IF(x IS NULL,1,0)+IF(y IS NULL,1,0)+IF(longitude IS NULL,1,0)+IF(latitude IS NULL,1,0)+IF(address IS NULL,1,0)+IF(city IS NULL,1,0)+IF(province IS NULL,1,0)+IF(bsic IS NULL,1,0)+IF(locationcode IS NULL,1,0)+IF(bsic IS NULL,1,0)+IF(BCCHNO IS NULL,1,0)+IF(file_date IS NULL,1,0) = NULL, 0, (IF(site IS NULL,1,0) + IF(cell IS NULL,1,0) + IF(cgi IS NULL,1,0)+IF(site_name IS NULL,1,0)+IF(original_i_s_date IS NULL,1,0)+IF(antenna_type IS NULL,1,0)+IF(azimuth IS NULL,1,0)+IF(beamwidth IS NULL,1,0)+IF(x IS NULL,1,0)+IF(y IS NULL,1,0)+IF(longitude IS NULL,1,0)+IF(latitude IS NULL,1,0)+IF(address IS NULL,1,0)+IF(city IS NULL,1,0)+IF(province IS NULL,1,0)+IF(locationcode IS NULL,1,0)+IF(bsic IS NULL,1,0)+IF(BCCHNO IS NULL,1,0)+IF(file_date IS NULL,1,0))) as row_null_cnt FROM cdrdm.dim_cell_site_info_GSM_temp1
    ) as tmp 
   ) as tmp2 where tmp2.row_cnt = 1
) as a
LEFT JOIN   (select x.* from (select *, ROW_NUMBER() OVER (PARTITION BY cgi ORDER BY file_date DESC) as hist_max_row from cdrdm.historical_dim_cell_site_info ) as x where x.hist_max_row = 1) as b 
on          TRIM(UPPER(a.cgi)) = TRIM(UPPER(b.cgi))
WHERE       trim(b.cgi) IS NOT NULL 
            AND 
            (
                trim(UPPER(a.site))<> trim(UPPER(b.site)) 
                or trim(UPPER(a.cell))<> trim(UPPER(b.cell)) 
                --or trim(UPPER(a.nodeb))<> trim(UPPER(b.nodeb)) 
                --or trim(UPPER(a.cgi_hex))<> trim(UPPER(b.cgi_hex))
                or trim(UPPER(a.site_name))<> trim(UPPER(b.site_name)) 
                or trim(UPPER(a.original_i_s_date))<> trim(UPPER(b.original_i_s_date))
                or trim(UPPER(a.antenna_type))<> trim(UPPER(b.antenna_type)) 
                or trim(UPPER(a.azimuth))<> trim(UPPER(b.azimuth))
                or trim(UPPER(a.beamwidth))<> trim(UPPER(b.beamwidth)) 
                or trim(UPPER(a.x))<> trim(UPPER(b.x))
                or trim(UPPER(a.y))<> trim(UPPER(b.y)) 
                or trim(UPPER(a.longitude))<> trim(UPPER(b.longitude))
                or trim(UPPER(a.latitude))<> trim(UPPER(b.latitude)) 
                or trim(UPPER(a.address))<> trim(UPPER(b.address))
                or trim(UPPER(a.city))<> trim(UPPER(b.city)) 
                or trim(UPPER(a.province))<> trim(UPPER(b.province))
                --or trim(UPPER(a.arfcndl))<> trim(UPPER(b.arfcndl)) 
                or trim(UPPER(a.bcchno))<> trim(UPPER(b.bcchno))
                or trim(UPPER(a.locationcode))<> trim(UPPER(b.locationcode)) 
                or trim(UPPER(a.bsic))<> trim(UPPER(b.bsic))
                --or trim(UPPER(a.primaryscramblingcode))<> trim(UPPER(b.primaryscramblingcode)) 
                --or trim(UPPER(a.file_type))<> trim(UPPER(b.file_type))
                --or trim(UPPER(a.insert_timestamp))<> trim(UPPER(b.insert_timestamp)) 
                --or trim(UPPER(a.file_date))<> trim(UPPER(b.file_date))
            )
;



drop view if exists cdrdm.vw_dim_cell_site_info ;
CREATE VIEW IF NOT EXISTS cdrdm.vw_dim_cell_site_info as
select 
a.file_name,
a.site,             
a.cell,             
a.nodeb,           
a.cgi,              
a.cgi_hex,          
a.site_name,        
a.original_i_s_date,       
a.antenna_type,       
a.azimuth,          
a.beamwidth,        
a.x,                
a.y,                
a.longitude,        
a.latitude,         
a.address,          
a.city,             
a.province,         
a.arfcndl,          
a.bcchno,           
a.locationcode,       
a.bsic,             
a.primaryscramblingcode,       
a.file_type,        
a.insert_timestamp, 
a.file_date as start_date,       
if(if(date_sub(LEAD(a.file_date) OVER (PARTITION BY a.cgi ORDER BY a.file_date),1) IS NULL, '2099-12-31', date_sub(LEAD(a.file_date) OVER (PARTITION BY a.cgi ORDER BY a.file_date),1)) < a.file_date, a.file_date, if(date_sub(LEAD(a.file_date) OVER (PARTITION BY a.cgi ORDER BY a.file_date),1) IS NULL, '2099-12-31', date_sub(LEAD(a.file_date) OVER (PARTITION BY a.cgi ORDER BY a.file_date),1))) as end_date,
a.file_date 
from 
(
select * from cdrdm.historical_dim_cell_site_info  
) as a;
 

--ALTER TABLE INSTEAD OF DROP TABLE HERE.

TRUNCATE TABLE cdrdm.dim_cell_site_info;
-- drop table if exists cdrdm.dim_cell_site_info;
-- CREATE TABLE IF NOT EXISTS `cdrdm.dim_cell_site_info`(
--  `file_name` varchar(50),
--  `site` varchar(15),
--  `cell` varchar(15),
--  `nodeb` varchar(10),
--  `cgi` varchar(35),
--  `cgi_hex` varchar(35),
-- `site_name` varchar(100),
--  `original_i_s_date` varchar(40),
--  `antenna_type` varchar(15),
--  `azimuth` varchar(10),
--  `beamwidth` varchar(10),
--  `x` varchar(20),
--  `y` varchar(20),
--  `longitude` varchar(25),
--  `latitude` varchar(25),
--  `address` varchar(254),
--  `city` varchar(50),
--  `province` varchar(10),
--  `arfcndl` varchar(10),
--  `bcchno` varchar(10),
--  `locationcode` varchar(15),
--  `bsic` varchar(10),
--  `primaryscramblingcode` varchar(10),
--  `file_type` varchar(10),
--  `insert_timestamp` string,
--  `Start_date` string, --new column
--  `End_date` string, --new column
--  `file_date` string)
-- ROW FORMAT DELIMITED
--  FIELDS TERMINATED BY '\u0001'
-- STORED AS ORC;


INSERT INTO TABLE cdrdm.dim_cell_site_info 
select
a.file_name      
,a.site             
,a.cell             
,a.nodeb           
,a.cgi              
,a.cgi_hex          
,a.site_name        
,a.original_i_s_date       
,a.antenna_type       
,a.azimuth          
,a.beamwidth        
,a.x                
,a.y                
,a.longitude        
,a.latitude         
,a.address          
,a.city             
,a.province         
,a.arfcndl          
,a.bcchno           
,a.locationcode       
,a.bsic             
,a.primaryscramblingcode       
,a.file_type        
,a.insert_timestamp 
,a.start_date       
,a.end_date 
,a.file_date
 from cdrdm.vw_dim_cell_site_info as a;

--************************************************************************************************
--************************************************************************************************
--** END OF CODE
--************************************************************************************************
--************************************************************************************************





