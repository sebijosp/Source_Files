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

USE cdrdm;

DROP TABLE Z_FACT_GMPC_CDR_Temp;

CREATE TABLE Z_FACT_GMPC_CDR_Temp(
--trafficcase SMALLINT,
trafficcase STRING,
--locationtype SMALLINT,
locationtype STRING,
clientid VARCHAR(50),
clientno VARCHAR(20),
pushurl VARCHAR(350),
pushid VARCHAR(100),
errorcode1 SMALLINT,
--clienttype SMALLINT,
clienttype STRING,
privacyoverride SMALLINT,
--coordinatesystem SMALLINT,
coordinatesystem STRING,
--datum SMALLINT,
datum STRING,
--requestedaccuracy SMALLINT,
requestedaccuracy STRING,
requestedaccuracymeters INT,
--responsetime SMALLINT,
responsetime STRING,
requestedpositiontime STRING,
subclientno VARCHAR(20),
clientrequestor VARCHAR(20),
clientservicetype SMALLINT,
targetms VARCHAR(20),
numbermsc VARCHAR(20),
numbersgsn VARCHAR(20),
positiontime STRING,
levelofconfidence SMALLINT,
obtainedaccuracy INT,
errorcode2 SMALLINT,
dialledbyms VARCHAR(20),
numbervlr VARCHAR(20),
--requestbearer SMALLINT,
requestbearer STRING,
setid VARCHAR(100),
latitude_v VARCHAR(100),
longitude_v VARCHAR(100),
latitude DECIMAL(10,5),
longitude DECIMAL(10,5),
province_id SMALLINT,
--network SMALLINT,
network STRING,
--usedlocationmethod SMALLINT,
usedlocationmethod STRING,
--time_key INT,
--audit_key INT,
esrd VARCHAR(25),
NumberMME VARCHAR(128),
CallDuration INT,
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
PARTITIONED BY (position_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY");

INSERT INTO TABLE Z_FACT_GMPC_CDR_Temp PARTITION (position_date) 
SELECT 
trafficcase,
locationtype,
clientid,
clientno,
pushurl,
pushid,
errorcode1,
clienttype,
privacyoverride,
coordinatesystem,
datum,
requestedaccuracy,
requestedaccuracymeters,
responsetime,
requestedpositiontime,
subclientno,
clientrequestor,
clientservicetype,
targetms,
numbermsc,
numbersgsn,
positiontime,
levelofconfidence,
obtainedaccuracy,
errorcode2,
dialledbyms,
numbervlr,
requestbearer,
setid,
latitude_v,
longitude_v,
latitude,
longitude,
province_id,
network,
usedlocationmethod,
esrd,
numbermme,
callduration,
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
position_date
FROM FACT_GMPC_CDR WHERE substr(requestedpositiontime,1,6) <> '2016--';