use cdrdm;
set hive.enforce.bucketing=true;
SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
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
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.min.partition.factor=0.25;
set hive.tez.max.partition.factor=2.0;
SET mapred.max.split.size=1000000;
SET mapred.compress.map.output=true;
SET mapred.output.compress=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.exec.parallel=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.dbclass=fs;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
SET hive.exec.max.dynamic.partitions.pernode=15000;
set hive.exec.max.dynamic.partitions=15000;
set hive.exec.reducers.bytes.per.reducer=10432;
set tez.shuffle-vertex-manager.min-src-fraction=0.25;
set tez.shuffle-vertex-manager.max-src-fraction=0.75;
set tez.runtime.report.partition.stats=true;
set hive.exec.reducers.bytes.per.reducer=1073741824;
set hive.optimize.index.filter=true;
set hive.optimize.ppd.storage=true;


drop table if exists cdrdm.DIM_CELL_SITE_INFO_LATEST_STG;
create table cdrdm.DIM_CELL_SITE_INFO_LATEST_STG stored as orc TBLPROPERTIES("orc.compress"="SNAPPY")
as SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) AS rank_file_date FROM cdrdm.DIM_CELL_SITE_INFO) X WHERE X.rank_file_date=1;

DROP TABLE IF EXISTS CDRDM.LES_FC_XREF;
 CREATE TABLE IF NOT EXISTS CDRDM.LES_FC_XREF
 (FIRST_CELL VARCHAR(35)
 ,CALL_DATE STRING)
 COMMENT 'XREF LIST OF FIRST CELL  FOR SIMPLE JOIN TO DIM CELL SITE INFO'
  PARTITIONED BY (SRC_TABLE_NAME STRING,CALL_TIMESTAMP_DATE STRING)
 stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");
 
 DROP TABLE IF EXISTS CDRDM.LES_LC_XREF;
 CREATE TABLE IF NOT EXISTS CDRDM.LES_LC_XREF
 (LAST_CELL VARCHAR(35)
 ,CALL_DATE STRING)
 COMMENT 'XREF LIST OF  LAST CELL FOR SIMPLE JOIN TO DIM CELL SITE INFO'
  PARTITIONED BY (SRC_TABLE_NAME STRING,CALL_TIMESTAMP_DATE STRING)
 stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");
 
 INSERT INTO CDRDM.LES_FC_XREF PARTITION(SRC_TABLE_NAME = 'LES_MAIN',CALL_TIMESTAMP_DATE)
 SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201512' AND FIRST_CELL_COMPLETE IS NOT NULL
 UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201601' AND FIRST_CELL_COMPLETE IS NOT NULL
   UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201602' AND FIRST_CELL_COMPLETE IS NOT NULL
   UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201603' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201604' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201605' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201606' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201607' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201608' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201609' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201610' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201611' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201612' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201701' AND FIRST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT FIRST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201702' AND FIRST_CELL_COMPLETE IS NOT NULL;
  
 
  INSERT INTO CDRDM.LES_FC_XREF PARTITION(SRC_TABLE_NAME = 'LES_IMM_INFO',CALL_TIMESTAMP_DATE)
 SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201512' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201601' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201602' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201603' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201604' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201605' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201606' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201607' AND A.First_Cell IS NOT NULL
 UNION
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201608' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201609' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201610' AND A.First_Cell IS NOT NULL
  UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201611' AND A.First_Cell IS NOT NULL
   UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201612' AND A.First_Cell IS NOT NULL
   UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201701' AND A.First_Cell IS NOT NULL
   UNION ALL
  SELECT DISTINCT A.First_Cell,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201702' AND A.First_Cell IS NOT NULL;
 
   INSERT INTO CDRDM.LES_LC_XREF PARTITION(SRC_TABLE_NAME = 'LES_IMM_INFO',CALL_TIMESTAMP_DATE)
 SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201512' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201601' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201602' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201603' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201604' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201605' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201606' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201607' and LAST_CELL IS NOT NULL
 UNION
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201608' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201609' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201610' and LAST_CELL IS NOT NULL
  UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201611' and LAST_CELL IS NOT NULL
   UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201612' and LAST_CELL IS NOT NULL
   UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201701' and LAST_CELL IS NOT NULL
   UNION ALL
  SELECT DISTINCT LAST_CELL,substr(A.recordopeningtime,1,10),A.srvcrequestdttmnrml_date
 FROM cdrdm.LES_IMM_INFO A WHERE DATE_FORMAT(A.srvcrequestdttmnrml_date,'YYYYMM') = '201702'  and LAST_CELL IS NOT NULL;
 
  INSERT INTO CDRDM.LES_LC_XREF PARTITION(SRC_TABLE_NAME = 'LES_MAIN',CALL_TIMESTAMP_DATE)
 SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201512' AND LAST_CELL_COMPLETE IS NOT NULL
 UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201601' AND LAST_CELL_COMPLETE IS NOT NULL
   UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201602' AND LAST_CELL_COMPLETE IS NOT NULL
   UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201603' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201604' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201605' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201606' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201607' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201608' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201609' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201610' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201611' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201612' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201701' AND LAST_CELL_COMPLETE IS NOT NULL
    UNION ALL
  SELECT DISTINCT LAST_CELL_COMPLETE,DATE_FORMAT(CALL_DATE,'YYYY-MM-DD'), CALL_TIMESTAMP_DATE
 FROM CDRDM.LES_MAIN WHERE DATE_FORMAT(CALL_TIMESTAMP_DATE,'YYYYMM') = '201702'  AND LAST_CELL_COMPLETE IS NOT NULL;
  

INSERT OVERWRITE TABLE cdrdm.DIM_CELL_SITE_LST_CELL_CMPLT_STG PARTITION(CALL_TIMESTAMP_DATE)
select a.file_name,a.site,a.cell,a.enodeb,a.cgi,a.cgi_hex,a.site_name,a.original_i,a.antenna_ty,a.azimuth,a.beamwidth,a.x,a.y,a.longitude,a.latitude,a.address,a.city,a.province,a.arfcndl,a.bcchno,a.locationco,a.bsic,a.primaryscr,a.file_type,a.insert_timestamp,a.start_date,a.end_date,a.file_date,b.last_cell,b.call_timestamp_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_LC_XREF B ON B.SRC_TABLE_NAME = 'LES_MAIN' and a.CGI_HEX = B.last_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

INSERT OVERWRITE TABLE cdrdm.DIM_CELL_SITE_FST_CELL_CMPLT_STG PARTITION(CALL_TIMESTAMP_DATE)
select  a.file_name,a.site,a.cell,a.enodeb,a.cgi,a.cgi_hex,a.site_name,a.original_i,a.antenna_ty,a.azimuth,a.beamwidth,a.x,a.y,a.longitude,a.latitude,a.address,a.city,a.province,a.arfcndl,a.bcchno,a.locationco,a.bsic,a.primaryscr,a.file_type,a.insert_timestamp,a.start_date,a.end_date,a.file_date,b.first_cell,b.call_timestamp_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_FC_XREF B ON B.SRC_TABLE_NAME = 'LES_MAIN' AND  a.CGI_HEX = B.FIRST_Cell
WHERE B.CALL_DATE BETWEEN a.START_DATE AND a.END_DATE;


drop table if exists cdrdm.DIM_CELL_SITE_LES_IMM_INFO_FC_STG;
create table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_FC_STG
(site                    varchar(15)
,cell                    varchar(15)
,enodeb                  varchar(10)
,cgi                     varchar(35)
,cgi_hex                 string
,site_name               varchar(100)
,original_i              varchar(40)
,antenna_ty              varchar(15)
,azimuth                 varchar(10)
,beamwidth               varchar(10)
,x                       varchar(20)
,y                       varchar(20)
,longitude               varchar(25)
,latitude                varchar(25)
,address                 varchar(254)
,city                    varchar(50)
,province                varchar(10)
,arfcndl                 varchar(10)
,bcchno                  varchar(10)
,locationco              varchar(15)
,bsic                    varchar(10)
,primaryscr              varchar(10)
,file_type               varchar(10)
,insert_timestamp        string
,file_date               string
,first_cell               string)
PARTITIONED BY (srvcrequestdttmnrml_date STRING) stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");

INSERT OVERWRITE table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_FC_STG  PARTITION (srvcrequestdttmnrml_date)
SELECT  A.site, A.cell, A.enodeb, A.cgi, upper(trim(A.cgi_hex)) as cgi_hex, A.site_name, A.original_i, A.antenna_ty, A.azimuth, A.beamwidth, A.x, A.y, A.longitude, A.latitude, A.address, A.city, A.province, A.arfcndl, A.bcchno, A.locationco, A.bsic, A.primaryscr, A.file_type, A.insert_timestamp, A.file_date,B.FIRST_CELL,B.CALL_TIMESTAMP_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_FC_XREF B ON B.SRC_TABLE_NAME = 'LES_IMM_INFO' AND  UPPER(TRIM(A.CGI_HEX)) = B.FIRST_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

drop table if exists cdrdm.DIM_CELL_SITE_LES_IMM_INFO_LC_STG;
create table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_LC_STG
(site                    varchar(15)
,cell                    varchar(15)
,enodeb                  varchar(10)
,cgi                     varchar(35)
,cgi_hex                 string
,site_name               varchar(100)
,original_i              varchar(40)
,antenna_ty              varchar(15)
,azimuth                 varchar(10)
,beamwidth               varchar(10)
,x                       varchar(20)
,y                       varchar(20)
,longitude               varchar(25)
,latitude                varchar(25)
,address                 varchar(254)
,city                    varchar(50)
,province                varchar(10)
,arfcndl                 varchar(10)
,bcchno                  varchar(10)
,locationco              varchar(15)
,bsic                    varchar(10)
,primaryscr              varchar(10)
,file_type               varchar(10)
,insert_timestamp        string
,file_date               string
,last_cell               string)
PARTITIONED BY (srvcrequestdttmnrml_date STRING) stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");

INSERT OVERWRITE table cdrdm.DIM_CELL_SITE_LES_IMM_INFO_lC_STG  PARTITION (srvcrequestdttmnrml_date)
SELECT  A.site, A.cell, A.enodeb, A.cgi, upper(trim(A.cgi_hex)) as cgi_hex, A.site_name, A.original_i, A.antenna_ty, A.azimuth, A.beamwidth, A.x, A.y, A.longitude, A.latitude, A.address, A.city, A.province, A.arfcndl, A.bcchno, A.locationco, A.bsic, A.primaryscr, A.file_type, A.insert_timestamp, A.file_date,B.last_CELL,B.CALL_TIMESTAMP_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_LC_XREF B ON B.SRC_TABLE_NAME = 'LES_IMM_INFO' AND UPPER(TRIM(A.CGI_HEX)) = B.last_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

drop view if exists cdrdm.vLesMain_New;
create view if not exists cdrdm.vLesMain_New
as select
A.call_type,
A.calling_number,
A.called_number,
A.original_called_number,
A.redirecting_number,
A.chargeable_duration,
A.First_Cell_complete,
A.Last_Cell_complete,
A.record_seq_number,
A.switch_name,
A.first_cell_id_extension,
A.subscriptionType,
A.SRVCCIndicator,
A.IsTbaytel,
A.CALL_DATE,
A.called_serial_number,
A.calling_serial_number,
A.imsi,
A.subject_number,
A.call_timestamp_date,
COALESCE(FCSite.file_name,FCSiteL.file_name) AS First_cell_file,
COALESCE(FCSite.Site,FCSiteL.Site)AS First_cell_site,
COALESCE(FCSite.Cell,FCSiteL.Cell) AS First_cell_cell,
COALESCE(FCSite.ANTENNA_TY,FCSiteL.ANTENNA_TY) AS First_cell_ANTENNA_TY,
COALESCE(FCSite.AZIMUTH,FCSiteL.AZIMUTH) AS First_cell_AZIMUTH,
COALESCE(FCSite.BEAMWIDTH,FCSiteL.BEAMWIDTH) AS First_cell_BEAMWIDTH,
COALESCE(FCSite.Site_name,FCSiteL.Site_name) AS First_cell_site_name,
COALESCE(FCSite.City,FCSiteL.City) AS First_cell_site_City_name,
COALESCE(FCSite.Province,FCSiteL.Province) AS First_cell_site_Prov_Name,
COALESCE(FCSite.Address,FCSiteL.Address) AS First_cell_site_Address,
COALESCE(FCSite.X,FCSiteL.X) AS First_cell_site_X,
COALESCE(FCSite.Y,FCSiteL.Y) AS First_cell_site_Y,
COALESCE(FCSite.Latitude,FCSiteL.Latitude) AS First_cell_site_Latitude,
COALESCE(FCSite.Longitude,FCSiteL.Longitude) AS First_cell_site_Longitude,
COALESCE(LCSite.file_name,LCSiteL.file_name) AS Last_cell_file,
COALESCE(LCSite.Site,LCSiteL.Site) AS Last_cell_site,
COALESCE(LCSite.Cell,LCSiteL.Cell) AS Last_cell_cell,
COALESCE(LCSite.ANTENNA_TY,LCSiteL.ANTENNA_TY) AS Last_cell_ANTENNA_TY,
COALESCE(LCSite.AZIMUTH,LCSiteL.AZIMUTH) AS Last_cell_AZIMUTH,
COALESCE(LCSite.BEAMWIDTH,LCSiteL.BEAMWIDTH) AS Last_cell_BEAMWIDTH,
COALESCE(LCSite.Site_name,LCSiteL.Site_name) AS Last_cell_site_name,
COALESCE(LCSite.City,LCSiteL.City) AS Last_cell_site_City_name,
COALESCE(LCSite.Province,LCSiteL.Province) AS Last_cell_site_Prov_Name,
COALESCE(LCSite.Address,LCSiteL.Address) AS Last_cell_site_Address,
COALESCE(LCSite.X,LCSiteL.X) AS Last_cell_site_X,
COALESCE(LCSite.Y,LCSiteL.Y) AS Last_cell_site_Y,
COALESCE(LCSite.Latitude,LCSiteL.Latitude) AS Last_cell_site_Latitude,
COALESCE(LCSite.Longitude,LCSiteL.Longitude)AS Last_cell_site_Longitude,
COALESCE(FCSite.CGI,FCSiteL.CGI) AS First_Cell,
COALESCE(LCSite.CGI,LCSiteL.CGI) AS Last_Cell
from cdrdm.Les_main A
LEFT OUTER JOIN cdrdm.DIM_CELL_SITE_FST_CELL_CMPLT_STG FCSite ON A.call_timestamp_date = FCSite.call_timestamp_date and A.First_Cell_complete = FCSite.cgi_hex
LEFT OUTER JOIN cdrdm.DIM_CELL_SITE_LST_CELL_CMPLT_STG LCSite ON A.call_timestamp_date = LCSite.call_timestamp_date and A.Last_Cell_complete = LCSite.cgi_hex
left outer join cdrdm.DIM_CELL_SITE_INFO_LATEST_STG FCSiteL ON  A.First_Cell_complete = FCSiteL.cgi_hex
LEFT OUTER JOIN cdrdm.DIM_CELL_SITE_INFO_LATEST_STG LCSiteL on  A.Last_Cell_complete = LCSiteL.cgi_hex;

drop view CDRDM.vLesImmINFO_New;
CREATE VIEW CDRDM.vLesImmINFO_New
as SELECT a.IsWifiUsage,a.RecordType,a.Retransmission,a.SIPMethod,a.RoleOfNode,a.NodeAddress,a.SessionId,a.CallingPartyAddress,a.CalledPartyAddress,a.SrvcRequestTimeStamp,a.SrvcDeliveryStartTimeStamp,a.SrvcDeliveryEndTimeStamp,a.RecordOpeningTime,a.RecordClosureTime,a.SrvcRequestDttmNrml,a.SrvcDeliveryStartDttmNrml,a.SrvcDeliveryEndDttmNrml,a.ChargeableDuration,a.TmFromSipRqstStartOfChrgng,a.InterOperatorIdentifiers,a.LocalRecordSequenceNumber,a.PartialOutputRecordNumber,a.CauseForRecordClosing,a.ACRStartLost,a.ACRInterimLost,a.ACRStopLost,a.IMSILost,a.ACRSCCASStartLost,a.ACRSCCASInterimLost,a.ACRSCCASStopLost,a.IMSChargingIdentifier,a.LstOfSDPMediaComponents,a.LstOfNrmlMediaC1ChgTime,a.LstOfNrmlMediaC1ChgTimeNor,a.LstOfNrmlMediaC1DescType1,a.LstOfNrmlMediaC1DescCodec1,a.LstOfNrmlMediaC1DescBndW1,a.LstOfNrmlMediaC1DescType2,a.LstOfNrmlMediaC1DescCodec2,a.LstOfNrmlMediaC1DescBndW2,a.LstOfNrmlMediaC1DescType3,a.LstOfNrmlMediaC1DescCodec3,a.LstOfNrmlMediaC1DescBndW3,a.LstOfNrmlMediaC1medInitFlg,a.LstOfNrmlMediaC2ChgTime,a.LstOfNrmlMediaC2ChgTimeNor,a.LstOfNrmlMediaC2DescType1,a.LstOfNrmlMediaC2DescCodec1,a.LstOfNrmlMediaC2DescBndW1,a.LstOfNrmlMediaC2DescType2,a.LstOfNrmlMediaC2DescCodec2,a.LstOfNrmlMediaC2DescBndW2,a.LstOfNrmlMediaC2DescType3,a.LstOfNrmlMediaC2DescCodec3,a.LstOfNrmlMediaC2DescBndW3,a.LstOfNrmlMediaC2medInitFlg,a.LstOfNrmlMediaC3ChgTime,a.LstOfNrmlMediaC3ChgTimeNor,a.LstOfNrmlMediaC3DescType1,a.LstOfNrmlMediaC3DescCodec1,a.LstOfNrmlMediaC3DescBndW1,a.LstOfNrmlMediaC3DescType2,a.LstOfNrmlMediaC3DescCodec2,a.LstOfNrmlMediaC3DescBndW2,a.LstOfNrmlMediaC3DescType3,a.LstOfNrmlMediaC3DescCodec3,a.LstOfNrmlMediaC3DescBndW3,a.LstOfNrmlMediaC3medInitFlg,a.LstOfNrmlMediaC4ChgTime,a.LstOfNrmlMediaC4ChgTimeNor,a.LstOfNrmlMediaC4DescType1,a.LstOfNrmlMediaC4DescCodec1,a.LstOfNrmlMediaC4DescBndW1,a.LstOfNrmlMediaC4DescType2,a.LstOfNrmlMediaC4DescCodec2,a.LstOfNrmlMediaC4DescBndW2,a.LstOfNrmlMediaC4DescType3,a.LstOfNrmlMediaC4DescCodec3,a.LstOfNrmlMediaC4DescBndW3,a.LstOfNrmlMediaC4medInitFlg,a.LstOfNrmlMediaC5ChgTime,a.LstOfNrmlMediaC5ChgTimeNor,a.LstOfNrmlMediaC5DescType1,a.LstOfNrmlMediaC5DescCodec1,a.LstOfNrmlMediaC5DescBndW1,a.LstOfNrmlMediaC5DescType2,a.LstOfNrmlMediaC5DescCodec2,a.LstOfNrmlMediaC5DescBndW2,a.LstOfNrmlMediaC5DescType3,a.LstOfNrmlMediaC5DescCodec3,a.LstOfNrmlMediaC5DescBndW3,a.LstOfNrmlMediaC5medInitFlg,a.LstOfNrmlMediaComponents6,a.LstOfNrmlMediaComponents7,a.LstOfNrmlMediaComponents8,a.LstOfNrmlMediaComponents9,a.LstOfNrmlMediaComponents10,a.LstOfNrmlMediaComponents11,a.LstOfNrmlMediaComponents12,a.LstOfNrmlMediaComponents13,a.LstOfNrmlMediaComponents14,a.LstOfNrmlMediaComponents15,a.LstOfNrmlMediaComponents16,a.LstOfNrmlMediaComponents17,a.LstOfNrmlMediaComponents18,a.LstOfNrmlMediaComponents19,a.LstOfNrmlMediaComponents20,a.LstOfNrmlMediaComponents21,a.LstOfNrmlMediaComponents22,a.LstOfNrmlMediaComponents23,a.LstOfNrmlMediaComponents24,a.LstOfNrmlMediaComponents25,a.LstOfNrmlMediaComponents26,a.LstOfNrmlMediaComponents27,a.LstOfNrmlMediaComponents28,a.LstOfNrmlMediaComponents29,a.LstOfNrmlMediaComponents30,a.LstOfNrmlMediaComponents31,a.LstOfNrmlMediaComponents32,a.LstOfNrmlMediaComponents33,a.LstOfNrmlMediaComponents34,a.LstOfNrmlMediaComponents35,a.LstOfNrmlMediaComponents36,a.LstOfNrmlMediaComponents37,a.LstOfNrmlMediaComponents38,a.LstOfNrmlMediaComponents39,a.LstOfNrmlMediaComponents40,a.LstOfNrmlMediaComponents41,a.LstOfNrmlMediaComponents42,a.LstOfNrmlMediaComponents43,a.LstOfNrmlMediaComponents44,a.LstOfNrmlMediaComponents45,a.LstOfNrmlMediaComponents46,a.LstOfNrmlMediaComponents47,a.LstOfNrmlMediaComponents48,a.LstOfNrmlMediaComponents49,a.LstOfNrmlMediaComponents50,a.LstOfNrmlMedCompts1150,a.GGSNAddress,a.ServiceReasonReturnCode,a.LstOfMessageBodies,a.RecordExtension,a.Expires,a.Event,a.Lst1AccessNetworkInfo,a.Lst1AccessDomain,a.Lst1AccessType,a.Lst1LocationInfoType,a.Lst1ChangeTime,a.Lst1ChangeTimeNormalized,b.site AS Lst1SITE,b.cell AS Lst1CELL,b.enodeb AS Lst1ENODEB,b.cgi AS Lst1CGI,b.cgi_hex AS Lst1CGI_HEX,b.site_name AS Lst1SITE_NAME,b.original_i AS Lst1ORIGINAL_I,b.antenna_ty AS Lst1ANTENNA_TY,b.azimuth AS Lst1AZIMUTH,b.beamwidth AS Lst1BEAMWIDTH,b.x AS Lst1X,b.y AS Lst1Y,b.longitude AS Lst1LONGITUDE,b.latitude AS Lst1LATITUDE,b.address AS Lst1ADDRESS,b.city AS Lst1CITY,b.province AS Lst1PROVINCE,b.arfcndl AS Lst1ARFCNDL,b.bcchno AS Lst1BCCHNO,b.locationco AS Lst1LOCATIONCO,b.bsic AS Lst1BSIC,b.primaryscr AS Lst1PRIMARYSCR,b.file_type AS Lst1FILE_TYPE,b.insert_timestamp AS Lst1INSERT_TIMESTAMP,b.file_date AS Lst1FILE_DATE,a.Last_Cell,c.site AS Last_Cell_SITE,c.cell AS Last_Cell_CELL,c.enodeb AS Last_Cell_ENODEB,c.cgi AS Last_Cell_CGI,c.cgi_hex AS Last_Cell_CGI_HEX,c.site_name AS Last_Cell_SITE_NAME,c.original_i AS Last_Cell_ORIGINAL_I,c.antenna_ty AS Last_Cell_ANTENNA_TY,c.azimuth AS Last_Cell_AZIMUTH,c.beamwidth AS Last_Cell_BEAMWIDTH,c.x AS Last_Cell_X,c.y AS Last_Cell_Y,c.longitude AS Last_Cell_LONGITUDE,c.latitude AS Last_Cell_LATITUDE,c.address AS Last_Cell_ADDRESS,c.city AS Last_Cell_CITY,c.province AS Last_Cell_PROVINCE,c.arfcndl AS Last_Cell_ARFCNDL,c.bcchno AS Last_Cell_BCCHNO,c.locationco AS Last_Cell_LOCATIONCO,c.bsic AS Last_Cell_BSIC,c.primaryscr AS Last_Cell_PRIMARYSCR,c.file_type AS Last_Cell_FILE_TYPE,c.insert_timestamp AS Last_Cell_INSERT_TIMESTAMP,c.file_date AS Last_Cell_FILE_DATE,a.Lst2AccessNetworkInfo,a.Lst2AccessDomain,a.Lst2AccessType,a.Lst2LocationInfoType,a.Lst2ChangeTime,a.Lst2ChangeTimeNormalized,a.Lst3AccessNetworkInfo,a.Lst3AccessDomain,a.Lst3AccessType,a.Lst3LocationInfoType,a.Lst3ChangeTime,a.Lst3ChangeTimeNormalized,a.Lst4AccessNetworkInfo,a.Lst4AccessDomain,a.Lst4AccessType,a.Lst4LocationInfoType,a.Lst4ChangeTime,a.Lst4ChangeTimeNormalized,a.Lst5AccessNetworkInfo,a.Lst5AccessDomain,a.Lst5AccessType,a.Lst5LocationInfoType,a.Lst5ChangeTime,a.Lst5ChangeTimeNormalized,a.Lst6AccessNetworkInfo,a.Lst6AccessDomain,a.Lst6AccessType,a.Lst6LocationInfoType,a.Lst6ChangeTime,a.Lst6ChangeTimeNormalized,a.Lst7AccessNetworkInfo,a.Lst7AccessDomain,a.Lst7AccessType,a.Lst7LocationInfoType,a.Lst7ChangeTime,a.Lst7ChangeTimeNormalized,a.Lst8AccessNetworkInfo,a.Lst8AccessDomain,a.Lst8AccessType,a.Lst8LocationInfoType,a.Lst8ChangeTime,a.Lst8ChangeTimeNormalized,a.Lst9AccessNetworkInfo,a.Lst9AccessDomain,a.Lst9AccessType,a.Lst9LocationInfoType,a.Lst9ChangeTime,a.Lst9ChangeTimeNormalized,a.Lst10AccessNetworkInfo,a.Lst10AccessDomain,a.Lst10AccessType,a.Lst10LocationInfoType,a.Lst10ChangeTime,a.Lst10ChangeTimeNormalized,a.ServiceContextID,a.SubscriberE164,a.SubscriberNo,a.IMSI,a.IMEI,a.SubSIPURI,a.NAI,a.SubPrivate,a.SubServedPartyDeviceID,a.LstOfEarlySDPMediaCmpnts,a.IMSCommServiceIdentifier,a.NumberPortabilityRouting,a.CarrierSelectRouting,a.SessionPriority,a.RequestedPartyAddress,a.LstOfCalledAssertedID,a.MMTelInfoAnalyzedCallType,a.MTlInfoCalledAsrtIDPrsntSts,a.MTlInfoCllgPrtyAddrPrsntSts,a.MMTelInfoConferenceId,a.MMTelInfoDialAroundIndctr,a.MMTelInfoRelatedICID,a.MMTelSplmtrySrvcInfo1ID,a.MMTelSplmtrySrvcInfo1Act,a.MMTelSplmtrySrvcInfo1Redir,a.MMTelSplmtrySrvcInfo2ID,a.MMTelSplmtrySrvcInfo2Act,a.MMTelSplmtrySrvcInfo2Redir,a.MMTelInfoSplmtrySrvcInfo3,a.MMTelInfoSplmtrySrvcInfo4,a.MMTelInfoSplmtrySrvcInfo5,a.MMTelInfoSplmtrySrvcInfo6,a.MMTelInfoSplmtrySrvcInfo7,a.MMTelInfoSplmtrySrvcInfo8,a.MMTelInfoSplmtrySrvcInfo9,a.MMTelInfoSplmtrySrvcInfo10,a.MobileStationRoamingNumber,a.TeleServiceCode,a.TariffClass,a.pVisitedNetworkID,a.SrvcRequestDttmNrml_Date FROM cdrdm.LES_IMM_INFO a
LEFT OUTER JOIN cdrdm.DIM_CELL_SITE_LES_IMM_INFO_FC_STG b
on a.SrvcRequestDttmNrml_Date = b.SrvcRequestDttmNrml_Date
and a.first_cell = b.cgi_hex
LEFT OUTER JOIN cdrdm.DIM_CELL_SITE_LES_IMM_INFO_LC_STG c
on a.SrvcRequestDttmNrml_Date = b.SrvcRequestDttmNrml_Date
and a.last_cell = c.cgi_hex;

truncate table cdrdm.les_main_fact_sms_cdr_step1;

truncate table cdrdm.les_main_fact_sms_cdr_step2;

truncate table cdrdm.les_main_sms_bkp;

