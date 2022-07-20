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

CREATE TABLE cdrdm.Z_FACT_PGW_DROP_CDR_Temp(
ERROR_CODES VARCHAR(40),
GPRS_CHOICE_MASK VARCHAR(5),
SERVED_IMSI VARCHAR(15),
RECORD_OPENING_TIME STRING,
SERVED_MSISDN VARCHAR(20),
DATA_VOLUME_GPRS_UPLINK INT,
DATA_VOLUME_GPRS_DOWNLINK INT,
TRACKING_AREA_CODE VARCHAR(5),
ACCESS_POINT_NAME VARCHAR(64),
ENODE VARCHAR(8),
CHARGING_ID VARCHAR(10),
DURATION INT,
CAUSE_FOR_TERM INT,
PGW_IP_ADDRESS VARCHAR(45),
SGW_IP_ADDRESS VARCHAR(45),
SWITCH_ID VARCHAR(7),
SERVED_IMEI VARCHAR(15),
--AUDIT_KEY BIGINT,
--INSRT_TMSTMP STRING,
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
PARTITIONED BY (RECORD_OPENING_DATE STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY");

CREATE VIEW ext_cdrdm.Z_FACT_PGW_DROP_CDR_Temp_View AS 
SELECT * FROM cdrdm.FACT_PGW_DROP_CDR WHERE error_codes = '817' OR error_codes = '1504' OR error_codes = '1505' OR error_codes = '1506';

INSERT INTO TABLE cdrdm.Z_FACT_PGW_DROP_CDR_Temp PARTITION (record_opening_date) SELECT * FROM ext_cdrdm.Z_FACT_PGW_DROP_CDR_Temp_View;