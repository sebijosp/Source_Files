USE cdrdm;

DROP TABLE FACT_GMPC_CDR;

CREATE TABLE FACT_GMPC_CDR(
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

USE ext_cdrdm;

CREATE TABLE IF NOT EXISTS cdr_processing
(
   tmp_file_name STRING
)
PARTITIONED BY (tmp_source STRING, tmp_destination STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
