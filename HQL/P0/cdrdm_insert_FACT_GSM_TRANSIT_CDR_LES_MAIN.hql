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

drop table if exists cdrdm.les_main_fact_gsm_trnst_cdr_step1;

create table cdrdm.les_main_fact_gsm_trnst_cdr_step1 stored as orc as
SELECT
   cleansed_calling_number AS subject_number,
   CAST(CONCAT('Transit CDR - ',(CASE  WHEN route_id IN ('DACCI', 'STRHI') THEN route_id ELSE 'VMR'  END)) AS VARCHAR(40)) AS call_type,
   CAST(NULL AS VARCHAR(16)) AS calling_serial_number,
   CAST(NULL AS VARCHAR(16)) AS called_serial_number,
   CAST(NULL AS BIGINT) imsi,
   call_timestamp AS call_timestamp,
   chargeable_duration AS chargeable_duration,
   cleansed_calling_number AS calling_number,
   cleansed_called_number AS called_number,
   cleansed_original_number AS original_called_number,
   cleansed_redirecting_number AS redirecting_number,
   SUBSTR(exchange_identity, 1, (instr( exchange_identity, '_') -1 )) AS switch_id_curr,
   CAST('0' AS STRING) AS switch_id_hist,
   CAST('0' AS VARCHAR(18)) AS first_cell_curr_formatted,
    CAST('0' AS CHAR(18)) AS first_cell_curr,
   CAST('0' AS CHAR(14)) AS first_cell_hist,
   CAST('0' AS VARCHAR(18)) AS last_cell_curr_formatted,
   CAST('0' AS VARCHAR(18)) AS last_cell_curr,
   CAST('0' AS CHAR(14)) AS last_cell_hist,
   record_sequence_number AS record_seq_number,
   1  AS source_type,
   'fact_gsm_transit_cdr'  AS source_table,
   CAST(NULL AS VARCHAR(4))  AS first_cell_id_extension,
    CAST(NULL AS CHAR(4)) subscriptionType,
    CAST (NULL AS CHAR(2)) AS SRVCCIndicator,
  trim(call_timestamp_date) as call_timestamp_date
   FROM cdrdm.fact_gsm_trnst_cdr_stg X1,
    (
    SELECT route_id FROM ela_v21.route_gg WHERE route_type ='R'
    UNION ALL
    SELECT 'DACCI' AS route_id
    UNION ALL
    SELECT 'STRHI' AS route_id
    ) Y
   WHERE X1.incoming_route_id = Y.route_id;

  drop table if exists cdrdm.les_main_fact_gsm_trnst_cdr_step2;
  create table cdrdm.les_main_fact_gsm_trnst_cdr_step2 stored as orc as
SELECT
   v_les_hist_all.call_type,
   v_les_hist_all.call_timestamp AS CALL_DATE,
   v_les_hist_all.calling_number,
   v_les_hist_all.called_number,
   v_les_hist_all.original_called_number,
   v_les_hist_all.redirecting_number,
   v_les_hist_all.chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN v_les_hist_all.first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN v_les_hist_all.Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN v_les_hist_all.first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN v_les_hist_all.Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(v_les_hist_all.first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(v_les_hist_all.Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType,
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table
FROM cdrdm.les_main_fact_gsm_trnst_cdr_step1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);


INSERT into CDRDM.LES_MAIN PARTITION (call_TIMESTAMP_DATE)
SELECT LES_DATA.call_type,
LES_DATA.calling_number,
LES_DATA.called_number,
LES_DATA.original_called_number,
LES_DATA.redirecting_number,
LES_DATA.chargeable_duration,
LES_DATA.First_Cell_complete_Orig AS First_Cell_complete,
LES_DATA.Last_Cell_complete_Orig AS Last_Cell_complete,
LES_DATA.record_seq_number,
LES_DATA.switch_name,
LES_DATA.first_cell_id_extension,
LES_DATA.subscriptionType,
LES_DATA.SRVCCIndicator,
CASE WHEN (LES_DATA.imsi BETWEEN 302720392000000 AND 302720392999999 OR LES_DATA.imsi BETWEEN 302720592000000 AND 302720592999999 OR LES_DATA.imsi BETWEEN 302720692000000 AND 302720692999999) AND Substr_First_Cell_complete_Orig IN ('8791','87F5','FCBC','FD20','FCC1','FD25') OR substr_Last_Cell_complete_Orig IN ('8791','87F5','FCBC','FD20','FCC1','FD25') THEN 'D' WHEN (Substr_First_Cell_complete_Orig IN ('8791','87F5','FCBC','FD20','FCC1','FD25') OR substr_Last_Cell_complete_Orig IN ('8791','87F5','FCBC','FD20','FCC1','FD25')) THEN 'R' ELSE NULL END AS IsTbaytel,
LES_DATA.CALL_DATE,
current_timestamp,
LES_DATA.called_serial_number,
LES_DATA.calling_serial_number,
LES_DATA.imsi,
LES_DATA.subject_number,
LES_DATA.source_table,
LES_DATA.call_timestamp_date
FROM CDRDM.les_main_fact_gsm_trnst_cdr_step2 LES_DATA;

INSERT INTO CDRDM.LES_FC_XREF PARTITION(SRC_TABLE_NAME = 'LES_MAIN',CALL_TIMESTAMP_DATE)
SELECT DISTINCT A.first_Cell_complete_Orig,SUBSTR(A.CALL_DATE,1,10), A.CALL_TIMESTAMP_DATE
 FROM CDRDM.les_main_fact_gsm_trnst_cdr_step2 A WHERE  A.first_Cell_complete_Orig IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM CDRDM.LES_FC_XREF B
 WHERE B.SRC_TABLE_NAME = 'LES_MAIN'
 AND B.CALL_TIMESTAMP_DATE = A.CALL_TIMESTAMP_DATE
 AND B.FIRST_Cell = A.FIRST_Cell_complete_Orig)
 ;
 
 INSERT INTO CDRDM.LES_LC_XREF PARTITION(SRC_TABLE_NAME = 'LES_MAIN',CALL_TIMESTAMP_DATE)
 SELECT DISTINCT A.Last_Cell_complete_Orig,SUBSTR(A.CALL_DATE,1,10), A.CALL_TIMESTAMP_DATE
 FROM CDRDM.les_main_fact_gsm_trnst_cdr_step2 A WHERE  A.Last_Cell_complete_Orig IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM CDRDM.LES_LC_XREF B
 WHERE B.SRC_TABLE_NAME = 'LES_MAIN'
 AND B.CALL_TIMESTAMP_DATE = A.CALL_TIMESTAMP_DATE
 AND B.Last_Cell = A.Last_Cell_complete_Orig)
 ;
 
INSERT OVERWRITE TABLE cdrdm.DIM_CELL_SITE_LST_CELL_CMPLT_STG PARTITION(CALL_TIMESTAMP_DATE)
select a.file_name,a.site,a.cell,a.enodeb,a.cgi,a.cgi_hex,a.site_name,a.original_i,a.antenna_ty,a.azimuth,a.beamwidth,a.x,a.y,a.longitude,a.latitude,a.address,a.city,a.province,a.arfcndl,a.bcchno,a.locationco,a.bsic,a.primaryscr,a.file_type,a.insert_timestamp,a.start_date,a.end_date,a.file_date,b.last_cell,b.call_timestamp_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_LC_XREF B ON B.SRC_TABLE_NAME = 'LES_MAIN' and a.CGI_HEX = B.last_Cell
WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

INSERT OVERWRITE TABLE cdrdm.DIM_CELL_SITE_FST_CELL_CMPLT_STG PARTITION(CALL_TIMESTAMP_DATE)
select  a.file_name,a.site,a.cell,a.enodeb,a.cgi,a.cgi_hex,a.site_name,a.original_i,a.antenna_ty,a.azimuth,a.beamwidth,a.x,a.y,a.longitude,a.latitude,a.address,a.city,a.province,a.arfcndl,a.bcchno,a.locationco,a.bsic,a.primaryscr,a.file_type,a.insert_timestamp,a.start_date,a.end_date,a.file_date,b.first_cell,b.call_timestamp_date  from cdrdm.DIM_CELL_SITE_INFO a JOIN CDRDM.LES_FC_XREF B ON B.SRC_TABLE_NAME = 'LES_MAIN' AND  a.CGI_HEX = B.FIRST_Cell WHERE B.CALL_DATE  BETWEEN a.START_DATE AND a.END_DATE;

truncate table cdrdm.fact_gsm_trnst_cdr_stg;
truncate table cdrdm.les_main_fact_gsm_trnst_cdr_step2;
truncate table cdrdm.les_main_fact_gsm_trnst_cdr_step1;
analyze table cdrdm.les_main partition(call_timestamp_date) compute statistics;

