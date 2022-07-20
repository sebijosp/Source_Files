-- DIM_CELL_SITE_INFO FIX
--** Filename: one_time_HIST_DIM_CELL_SITE_INFO_RUN.hql
--** Created: October 14, 2016
--** Author: Lalit Summan
--** Version: 0.1
--** BACKS UP THE PREVIOUS DIM_CELL_SITE_INFO TABLE INTO A TEMP TABLE.
--** THIS HIVE SCRIPT CONTAINS ONE TIME LOAD FROM TERADATA.
--** CREATES A STAGING TABLE WITH 2 NEW COLUMNS: 1. START_DATE 2. END_DATE BASED ON HISTORICAL TD LOAD
--** RECREATES DIM_CELL_SITE_INFO BASED ON NEW STAGING TABLE WITH 2 NEW COLUMSN.
--************************************************************************************************
--************************************************************************************************
--************************************************************************************************



--************************************************************************************************
--************************************************************************************************
--**BACKUP EXISTING DIM_CELL_SITE_INFO TABLE
--************************************************************************************************
--************************************************************************************************

 CREATE TABLE if not exists cdrdm.old_dim_cell_site_info_2016_11_15 as
 select * from cdrdm.dim_cell_site_info;

--************************************************************************************************
--************************************************************************************************
--**INITIAL LOAD THE SQOOPED DATA FROM TD INTO AN EXTERNAL TABLE
--************************************************************************************************
--************************************************************************************************
drop table if exists cdrdm.dim_cell_site_info_ext;
CREATE EXTERNAL TABLE IF NOT EXISTS cdrdm.dim_cell_site_info_ext (
FILE_NAME VARCHAR(50) ,
SITE VARCHAR(15) ,
CELL VARCHAR(15) ,
ENODEB VARCHAR(10) ,
CGI VARCHAR(35) ,
CGI_HEX VARCHAR(35) ,
SITE_NAME VARCHAR(100) ,
ORIGINAL_I VARCHAR(40) ,
ANTENNA_TY VARCHAR(15) ,
AZIMUTH VARCHAR(10) ,
BEAMWIDTH VARCHAR(10) ,
X VARCHAR(20) ,
Y VARCHAR(20) ,
LONGITUDE VARCHAR(25) ,
LATITUDE VARCHAR(25) ,
ADDRESS VARCHAR(254) ,
CITY VARCHAR(50) ,
PROVINCE VARCHAR(10) ,
ARFCNDL VARCHAR(10) ,
BCCHNO VARCHAR(10) ,
LOCATIONCO VARCHAR(15) ,
BSIC VARCHAR(10) ,
PRIMARYSCR VARCHAR(10) ,
START_DATE  string,
END_DATE  string,
INSERT_TIMESTAMP  string,
UPDATE_TIMESTAMP  string,
WORKFLOW_RUN_ID DECIMAL(11,0))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
--ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    location '/userapps/cdrdm/landing/tdprod/CDR_PROD_MART/dim_cell_site_info/sqoop_ext_date=2016-11-15/'
;

drop table if exists cdrdm.history_dim_cell_site_info;
CREATE TABLE if not exists cdrdm.history_dim_cell_site_info (
FILE_NAME VARCHAR(50) ,
SITE VARCHAR(15) ,
CELL VARCHAR(15) ,
ENODEB VARCHAR(10) ,
CGI VARCHAR(35) ,
CGI_HEX VARCHAR(35) ,
SITE_NAME VARCHAR(100) ,
ORIGINAL_I VARCHAR(40) ,
ANTENNA_TY VARCHAR(15) ,
AZIMUTH VARCHAR(10) ,
BEAMWIDTH VARCHAR(10) ,
X VARCHAR(20) ,
Y VARCHAR(20) ,
LONGITUDE VARCHAR(25) ,
LATITUDE VARCHAR(25) ,
ADDRESS VARCHAR(254) ,
CITY VARCHAR(50) ,
PROVINCE VARCHAR(10) ,
ARFCNDL VARCHAR(10) ,
BCCHNO VARCHAR(10) ,
LOCATIONCO VARCHAR(15) ,
BSIC VARCHAR(10) ,
PRIMARYSCR VARCHAR(10) ,
START_DATE  string,
END_DATE  string,
INSERT_TIMESTAMP  string,
UPDATE_TIMESTAMP  string,
WORKFLOW_RUN_ID DECIMAL(11,0)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0001';

INSERT INTO TABLE cdrdm.history_dim_cell_site_info
 select * from cdrdm.dim_cell_site_info_ext;
--************************************************************************************************
--************************************************************************************************
--**CREATE STAGING HIVE TABLE TO CONTAIN ALL THE HISTORY FROM TD
--**STAGING TEMP TABLE CONTAINS START_DATE AND END_DATE COLUMNS
--************************************************************************************************
--************************************************************************************************

-- drop table if exists cdrdm.dim_cell_site_info_ext_temp1;
-- CREATE TABLE IF NOT EXISTS cdrdm.dim_cell_site_info_ext_temp1 AS
-- select aa.*,
-- if(instr(substr(aa.temp_start_date,0,10),'/') = 0, substr(aa.temp_start_date,0,10), concat(substr(aa.temp_start_date,0,8), '0', substr(aa.temp_start_date,9,1)))  as start_date,
-- if(instr(substr(aa.temp_end_date,0,10),'/') = 0, substr(aa.temp_end_date,0,10), concat(substr(aa.temp_end_date,0,8), '0', substr(aa.temp_end_date,9,1)))  as end_date,
-- concat(substr(aa.temp_file_date, 0,4),'-', substr(aa.temp_file_date, 5,2), '-', substr(aa.temp_file_date, 7,2)) as file_date
-- from (
-- SELECT
-- file_name,
-- site,
-- cell,
-- enodeb,
-- cgi,
-- cgi_hex ,
-- site_name,
-- original_i,
-- antenna_ty,
-- azimuth ,
-- beamwidth,
-- x       ,
-- y       ,
-- longitude,
-- latitude,
-- address ,
-- city    ,
-- province,
-- arfcndl ,
-- bcchno  ,
-- locationco,
-- bsic    ,
-- primaryscr,
-- concat(substr((start_date),LOCATE('/',trim(start_date),4)+1, 4), '-', 
-- if(length(substr((start_date),1, LOCATE('/',trim(start_date),1) -1)) = 1,concat('0',substr((start_date),1, LOCATE('/',trim(start_date),1) -1)), substr((start_date),1, LOCATE('/',trim(start_date),1) -1)), '-', 
-- if(length(substr((start_date),LOCATE('/', TRIM(START_DATE),1)+1, LOCATE('/',trim(start_date),4)-3)) = 1, concat('0',substr((start_date),LOCATE('/', TRIM(START_DATE),1)+1, LOCATE('/',trim(start_date),3)-3)) ,substr((start_date),LOCATE('/', TRIM(START_DATE),1)+1, LOCATE('/',trim(start_date),4)-3))) as temp_start_date,
-- concat(substr((end_date),LOCATE('/',trim(end_date),4)+1, 4), '-', 
-- if(length(substr((end_date),1, LOCATE('/',trim(end_date),1) -1)) = 1,concat('0',substr((end_date),1, LOCATE('/',trim(end_date),1) -1)), substr((end_date),1, LOCATE('/',trim(end_date),1) -1)), '-', 
-- if(length(substr((end_date),LOCATE('/', TRIM(end_DATE),1)+1, LOCATE('/',trim(end_date),4)-3)) = 1, concat('0',substr((end_date),LOCATE('/', TRIM(end_DATE),1)+1, LOCATE('/',trim(end_date),3)-3)) ,substr((end_date),LOCATE('/', TRIM(end_DATE),1)+1, LOCATE('/',trim(end_date),4)-3))) as temp_end_date,
-- insert_timestamp       ,
-- update_timestamp       ,
-- workflow_run_id        ,
-- substr(file_name,LOCATE('base', TRIM(file_name),1)+4, LENGTH(trim(file_name)) -35) as file_type,
-- substr(substr(file_name,length(trim(file_name)) - 11, length(trim(file_name)) - 29),0,8)  as temp_file_date,
-- --CONCAT(substring(file_date,1,4),'-',substring(file_date,5,2),'-',substring(file_date,7,2)) as file_date_dash,
-- --if(length(file_name)=38, substr(file_name,23,3), substr(file_name,23,4)) as file_type,
-- LOCATE('-', trim(cgi)) as first_occurence,
-- LOCATE('-', trim(cgi), cast(LOCATE('-',trim(cgi)) as int)+1) as second_occurrence,
-- length(cgi) - LOCATE('-', reverse(trim(cgi))) + 1 as last_occurrence
-- FROM cdrdm.dim_cell_site_info_ext
-- ) as aa ;

drop table if exists cdrdm.dim_cell_site_info_staging;
CREATE TABLE `cdrdm.dim_cell_site_info_staging`(
`file_name` varchar(50),
`site` varchar(15),
`cell` varchar(15),
`enodeb` varchar(10),
`cgi` varchar(35),
`cgi_hex` varchar(35),
`site_name` varchar(100),
`original_i` varchar(40),
`antenna_ty` varchar(15),
`azimuth` varchar(10),
`beamwidth` varchar(10),
`x` varchar(20),
`y` varchar(20),
`longitude` varchar(25),
`latitude` varchar(25),
`address` varchar(254),
`city` varchar(50),
`province` varchar(10),
`arfcndl` varchar(10),
`bcchno` varchar(10),
`locationco` varchar(15),
`bsic` varchar(10),
`primaryscr` varchar(10),
`file_type` varchar(10),
`insert_timestamp` string,
-- `start_date` string,
-- `end_date` string,
`file_date` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE;


--************************************************************************************************
--************************************************************************************************
--** INSERT HISTORY DIM_CELL_SITE_INFO TABLE INTO TEMPORARY STAGING IN HIVE WITH 2 NEW COLUMNS AND COLUMN LOGIC
--************************************************************************************************
--************************************************************************************************

INSERT INTO TABLE cdrdm.dim_cell_site_info_staging
SELECT
file_name,
site,
cell,
enodeb,
cgi,
cgi_hex,
site_name,
original_i,
antenna_ty,
azimuth,
beamwidth,
x,
y,
longitude,
latitude,
address,
city,
province,
arfcndl,
bcchno,
locationco,
bsic,
primaryscr,
substr(file_name,LOCATE('base', TRIM(file_name),1)+4, LENGTH(trim(file_name)) -35) as file_type,
--file_type,
insert_timestamp,
-- start_date,
-- end_date,
concat(substr(substr(trim(file_name), LOCATE('_', TRIM(file_name))+1, LOCATE('.', TRIM(file_name))),0,4), '-', substr(substr(trim(file_name), LOCATE('_', TRIM(file_name))+1, LOCATE('.', TRIM(file_name))),5,2), '-',substr(substr(trim(file_name), LOCATE('_', TRIM(file_name))+1, LOCATE('.', TRIM(file_name))),7,2)) as file_date
--file_date
FROM cdrdm.dim_cell_site_info_ext;


--************************************************************************************************
--************************************************************************************************
--** DROP EXISTING DIM_CELL_SITE_INFO TABLE IN PRODUCTION
--************************************************************************************************
--************************************************************************************************
DROP TABLE IF EXISTS cdrdm.dim_cell_site_info;
CREATE TABLE IF NOT EXISTS `cdrdm.dim_cell_site_info`(
 `file_name` varchar(50),
 `site` varchar(15),
 `cell` varchar(15),
 `nodeb` varchar(10),
 `cgi` varchar(35),
 `cgi_hex` varchar(35),
`site_name` varchar(100),
 `original_i_s_date` varchar(40),
 `antenna_type` varchar(15),
 `azimuth` varchar(10),
 `beamwidth` varchar(10),
 `x` varchar(20),
 `y` varchar(20),
 `longitude` varchar(25),
 `latitude` varchar(25),
 `address` varchar(254),
 `city` varchar(50),
 `province` varchar(10),
 `arfcndl` varchar(10),
 `bcchno` varchar(10),
 `locationcode` varchar(15),
 `bsic` varchar(10),
 `primaryscramblingcode` varchar(10),
 `file_type` varchar(10),
 `insert_timestamp` string,
 `Start_date` string, --new column
 `End_date` string, --new column
 `file_date` string)
ROW FORMAT DELIMITED
 FIELDS TERMINATED BY '\u0001'
STORED AS ORC;


--************************************************************************************************
--************************************************************************************************
--** CREATE NEW DDL FOR DIM_CELL_SITE_INFO BASED ON TEMP STRUCTURE OF TEMP_DIM_CELL_SITE_INFO TABLE. 
--**NEW DIM_CELL_SITE_INFO WILL INCLUDE 2 NEW COLUMNS.
--************************************************************************************************
--************************************************************************************************
drop table if exists cdrdm.historical_dim_cell_site_info;
CREATE TABLE IF NOT EXISTS `cdrdm.historical_dim_cell_site_info`(
  `file_name` varchar(50),
  `site` varchar(15),
  `cell` varchar(15),
  `nodeb` varchar(10),
  `cgi` varchar(35),
  `cgi_hex` varchar(35),
  `site_name` varchar(100),
  `original_i_s_date` varchar(40),
  `antenna_type` varchar(15),
  `azimuth` varchar(10),
  `beamwidth` varchar(10),
  `x` varchar(20),
  `y` varchar(20),
  `longitude` varchar(25),
  `latitude` varchar(25),
  `address` varchar(254),
  `city` varchar(50),
  `province` varchar(10),
  `arfcndl` varchar(10),
  `bcchno` varchar(10),
  `locationcode` varchar(15),
  `bsic` varchar(10),
  `primaryscramblingcode` varchar(10),
  `file_type` varchar(10),
  `insert_timestamp` string,
  -- `Start_date` string, --new column
  -- `End_date` string, --new column
  `file_date` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
STORED AS ORC;

--************************************************************************************************
--************************************************************************************************
--** CREATE NEW DDL FOR DIM_CELL_SITE_INFO BASED ON TEMP STRUCTURE OF TEMP_DIM_CELL_SITE_INFO TABLE. 
--**NEW DIM_CELL_SITE_INFO WILL INCLUDE 2 NEW COLUMNS.
--************************************************************************************************
--************************************************************************************************


INSERT INTO TABLE cdrdm.historical_dim_cell_site_info 
 select * from cdrdm.dim_cell_site_info_staging;

--************************************************************************************************
--************************************************************************************************
--** END OF CODE
--************************************************************************************************
--************************************************************************************************






