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

alter table cdrdm.LES_MAIN rename to cdrdm.les_main_bkp;
create table if not exists cdrdm.LES_MAIN (
call_type VARCHAR(50),
calling_number varchar(30),
called_number varchar(30),
original_called_number bigint,
redirecting_number bigint,
chargeable_duration int,
First_Cell_complete varchar(25),
Last_Cell_complete varchar(25),
record_seq_number bigint,
switch_name varchar(50),
first_cell_id_extension varchar(4),
subscriptionType varchar(4),
SRVCCIndicator varchar(2),
isTbayTel char(1),
call_date string,
insert_timestamp string,
called_serial_number BIGINT,
calling_serial_number bigint,
imsi bigint,
subject_number bigint,
source_table string
)
partitioned by (call_timestamp_date STRING)
clustered by (subject_number) sorted by (subject_number) into 900 buckets
stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201512';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201601';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201602';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201603';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201604';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201605';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201606';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201607';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201608';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201609';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201610';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201611';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201612';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201701';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP1 AS
SELECT subscriber_no AS subject_number,
CAST(CASE WHEN call_type=2 THEN 'GSM Mobile Originating' WHEN call_type=3 THEN 'GSM Roaming Call Forward' WHEN call_type=4 THEN 'GSM Call Forward'  WHEN call_type=5 THEN 'GSM Mobile Terminating'  WHEN call_type=6 THEN 'GSM Originating SMS'  WHEN call_type=8 THEN 'GSM Terminating SMS'  ELSE 'Unknown' END AS VARCHAR(40)) AS call_type, CASE WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS calling_serial_number,
CASE WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16))) WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16))) ELSE NULL END AS called_serial_number,
imsi AS imsi,
call_timestamp AS call_timestamp,
chargeable_duration AS chargeable_duration,
cleansed_calling_number AS calling_number,
CASE WHEN call_type=4 THEN cleansed_redirecting_number WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no WHEN file_name="FACT_GSM_CF_HIST" THEN CAST(redirecting_number AS BIGINT) ELSE (CASE WHEN LENGTH(cleansed_called_number) < LENGTH(cleansed_redirecting_number) THEN cleansed_redirecting_number ELSE cleansed_called_number end) END AS called_number,
cleansed_original_number AS original_called_number,
CASE WHEN call_type=4 THEN cleansed_called_number WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT) ELSE cleansed_redirecting_number END AS redirecting_number,
SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
CASE WHEN file_name IN("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name ELSE CAST('0' AS STRING) END AS switch_id_hist,   
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", "")) ELSE CONCAT(SUBSTR(first_cell_id,1,6),regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""), regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), sUBSTR(TRIM(first_cell_id),11,4)) END) END AS first_cell_curr_formatted,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id ELSE(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id) ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END) END AS first_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0') ELSE CAST('0' AS CHAR(14)) END AS first_cell_hist,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" )) ELSE CONCAT(SUBSTR(last_cell_id,1,6),regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),        regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),SUBSTR(TRIM(last_cell_id),11,4)) END) END AS last_cell_curr_formatted, 
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) ELSE (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id) ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)  END AS last_cell_curr,
CASE WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0') ELSE CAST('0' AS CHAR(14)) END AS last_cell_hist,
record_sequence_number,
1 AS source_type,
'fact_gsm_cdr' AS source_table,
first_cell_id_extension AS first_cell_id_extension,
subscriptionType, 
SRVCCIndicator,
call_timestamp_date
 FROM cdrdm.fact_gsm_cdr WHERE  call_type <> 3 and date_format(call_timestamp_date,'YYYYMM') = '201702';
 
 DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
CREATE TABLE CDRDM.LES_MAIN_FACT_GSM_STEP2 stored as orc AS
SELECT call_type,
v_les_hist_all.call_timestamp AS call_date,
v_les_hist_all.calling_number AS calling_number,
v_les_hist_all.called_number AS called_number,
v_les_hist_all.original_called_number AS original_called_number,
v_les_hist_all.redirecting_number AS redirecting_number,
v_les_hist_all.chargeable_duration AS chargeable_duration,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr_formatted ELSE FC_Hist.LTE_HSPA_CellSite END AS First_Cell_complete,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr_formatted ELSE LC_HIST.LTE_HSPA_CellSite END AS Last_Cell_complete,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN first_cell_curr ELSE FC_Hist.LTE_HSPA_CellSite_Orig END AS First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN Last_cell_curr ELSE LC_HIST.LTE_HSPA_CellSite_Orig END AS Last_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.first_cell_hist='0') THEN SUBSTR(first_cell_curr,7,4) ELSE SUBSTR(FC_Hist.LTE_HSPA_CellSite_Orig,7,4) END AS Substr_First_Cell_complete_Orig,
CASE WHEN (v_les_hist_all.last_cell_hist='0') THEN SUBSTR(Last_cell_curr,7,4) ELSE SUBSTR(LC_HIST.LTE_HSPA_CellSite_Orig,7,4) END AS substr_Last_Cell_complete_Orig,
v_les_hist_all.record_sequence_number AS record_seq_number,
COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
v_les_hist_all.first_cell_id_extension,
v_les_hist_all.subscriptionType, 
v_les_hist_all.SRVCCIndicator,
v_les_hist_all.call_timestamp_date as call_timestamp_date,
cast(v_les_hist_all.imsi as bigint) AS imsi,
cast(v_les_hist_all.subject_number as bigint) AS subject_number,
cast(v_les_hist_all.called_serial_number as bigint) AS called_serial_number
,cast(v_les_hist_all.calling_serial_number as bigint) AS calling_serial_number,
v_les_hist_all.source_table as source_table
FROM CDRDM.LES_MAIN_FACT_GSM_STEP1 v_les_hist_all
LEFT OUTER JOIN cdrdm.v_measured_object FC_Hist ON (FC_Hist.measured_object_id=v_les_hist_all.first_cell_hist)
LEFT OUTER JOIN cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id=v_les_hist_all.last_cell_hist)
LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id=v_les_hist_all.switch_id_hist);
 

INSERT INTO TABLE CDRDM.LES_MAIN  PARTITION (call_TIMESTAMP_DATE)
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
FROM CDRDM.LES_MAIN_FACT_GSM_STEP2 LES_DATA;

truncate table cdrdm.fact_gsm_cdr_stg;
DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP2;
DROP TABLE IF EXISTS CDRDM.LES_MAIN_FACT_GSM_STEP1;
