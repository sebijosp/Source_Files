set hive.execution.engine=tez;

!echo Parms to Hive Script: filename='${hiveconf:file_name}';

create external table if not exists EXT_MT_PRICING.PRICING_PG_CODE(
PG_CODE	STRING,
GROUP_RANK	STRING,
CATEGORY	STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:file_name}'
tblproperties("skip.header.line.count"="1");

create table if not exists MT_PRICING.PRICING_PG_CODE
(
PG_CODE	STRING,
GROUP_RANK  INT,
CATEGORY  STRING,
FILE_NAME STRING,
HDP_CREATED_TS TIMESTAMP
)
partitioned by (LOAD_DATE DATE)
stored as ORC;

!echo loading the table MT_PRICING.PRICING_PG_CODE;

INSERT OVERWRITE TABLE MT_PRICING.PRICING_PG_CODE PARTITION(LOAD_DATE)
SELECT
PG_CODE,
cast(GROUP_RANK as int),
CATEGORY,
trim(regexp_extract(input__file__name,'[^/]+$', 0)),
current_timestamp,
cast(substring(from_unixtime(unix_timestamp(current_timestamp,'MM/dd/yyyy')),1,10) as date) as LOAD_DATE
FROM
EXT_MT_PRICING.PRICING_PG_CODE;
