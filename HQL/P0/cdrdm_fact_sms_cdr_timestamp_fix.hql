--SET hive.variable.substitute=true;
SET hive.auto.convert.join=false;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=mr;
--SET hive.vectorized.execution.enabled=false;
--SET hive.vectorized.execution.reduce.enabled=false;
--SET hive.cbo.enable=true;
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
SET hive.optimize.index.filter=false;

USE cdrdm;

DROP TABLE Z_FACT_SMS_CDR_Temp;

CREATE TABLE Z_FACT_SMS_CDR_Temp(
subscriber_no BIGINT,
event_record_id SMALLINT,
spid SMALLINT,
orig_msc VARCHAR(30),
other_msisdn BIGINT,
orig_msc_zone_indicator INT,
rate SMALLINT,
charged_party CHAR(1),
action SMALLINT,
rule INT,
transaction_id INT,
charge_result SMALLINT,
balance INT,
rate_plan SMALLINT,
on_net CHAR(1),
originating_service_grade INT,
terminating_service_grade INT,
sequence_number INT,
local_subscriber_timestamp STRING,
imsi BIGINT,
ban BIGINT,
language_1 CHAR(1),
bill_cycle_date SMALLINT,
orignating_subscriber_loc CHAR(2),
terminating_subscriber_loc CHAR(2),
rating_rule_description_1 VARCHAR(30),
rating_rule_description_2 VARCHAR(30),
origination_eq_type CHAR(1),
destination_eq_type CHAR(1),
date_1 STRING,
--switch_id SMALLINT,
--time_key INT,
--audit_key INT,
ocn CHAR(4),
other_msisdn_sc VARCHAR(10),
TR_Type_OMSC CHAR(1),
TR_OMSC_PLMN VARCHAR(7),
TR_LAC_TAC VARCHAR(5),
TR_ECID_CLID VARCHAR(10),
TR_MSC_TYPE VARCHAR(5),
TR_DCODE_COUNTRY VARCHAR(30),
TR_ACCOUNT_SUB_TYPE CHAR(1),
TR_COMPANY_CODE VARCHAR(15),
TR_FRANCHISE_CD VARCHAR(5),
TR_BA_ACCOUNT_TYPE CHAR(1),
MTYPE VARCHAR(5),
file_name STRING,
record_start BIGINT,
record_end BIGINT,
record_type STRING,
family_name STRING,
version_id INT,
file_time STRING,
file_id STRING,
switch_name STRING,
num_records STRING,
insert_timestamp STRING)
PARTITIONED BY (local_subscriber_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY");

INSERT INTO TABLE Z_FACT_SMS_CDR_Temp PARTITION (local_subscriber_date) 
SELECT 
subscriber_no,
event_record_id,
spid,
orig_msc,
other_msisdn,
orig_msc_zone_indicator,
rate,
charged_party,
action,
rule,
transaction_id,
charge_result,
balance,
rate_plan,
on_net,
originating_service_grade,
terminating_service_grade,
sequence_number,
substr(local_subscriber_timestamp,1,19) as local_subscriber_timestamp,
imsi,
ban,
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
substr(date_1,1,19) as date_1,
ocn,
other_msisdn_sc,
tr_type_omsc,
tr_omsc_plmn,
tr_lac_tac,
tr_ecid_clid,
tr_msc_type,
tr_dcode_country,
tr_account_sub_type,
tr_company_code,
tr_franchise_cd,
tr_ba_account_type,
mtype,
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
insert_timestamp,
local_subscriber_date
FROM FACT_SMS_CDR where file_name IN ('FACT_SMS_CDR', 'FACT_SMS_HISTORY');

INSERT INTO TABLE Z_FACT_SMS_CDR_Temp PARTITION (local_subscriber_date) 
SELECT
subscriber_no,
event_record_id,
spid,
orig_msc,
other_msisdn,
orig_msc_zone_indicator,
rate,
charged_party,
action,
rule,
transaction_id,
charge_result,
balance,
rate_plan,
on_net,
originating_service_grade,
terminating_service_grade,
sequence_number,
concat(substr(local_subscriber_timestamp,1,4),'-',substr(local_subscriber_timestamp,6,2),'-',substr(local_subscriber_timestamp,9,2),' ',substr(local_subscriber_timestamp,12,8)) as local_subscriber_timestamp,
imsi,
ban,
language_1,
bill_cycle_date,
orignating_subscriber_loc,
terminating_subscriber_loc,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
concat(substr(date_1,1,4),'-',substr(date_1,6,2),'-',substr(date_1,9,2),' ',substr(date_1,12,8)) as date_1,
ocn,
other_msisdn_sc,
tr_type_omsc,
tr_omsc_plmn,
tr_lac_tac,
tr_ecid_clid,
tr_msc_type,
tr_dcode_country,
tr_account_sub_type,
tr_company_code,
tr_franchise_cd,
tr_ba_account_type,
mtype,
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
insert_timestamp,
local_subscriber_date
FROM FACT_SMS_CDR where file_name NOT IN ('FACT_SMS_CDR', 'FACT_SMS_HISTORY');