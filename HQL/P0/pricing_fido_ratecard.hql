set hive.execution.engine=tez;

!echo Parms to Hive Script: filename='${hiveconf:file_name}';

create external table if not exists EXT_MT_PRICING.FIDO_TVM_PRICING(
REGION    STRING,
PLAN_CATEGORY    STRING,
PLAN_TYPE    STRING,
SOC   STRING,
TYPE    STRING,
MSF    STRING,
DATA_BUCKET    STRING,
DATA_BUCKET_DESC    STRING,
SUBSIDY    STRING,
PLAN_DESCRIPTION    STRING,
SALES_DESC    STRING,
FOR_SALE    STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:file_name}'
tblproperties("skip.header.line.count"="1");

create table if not exists MT_PRICING.FIDO_TVM_PRICING
(
REGION    STRING,
PLAN_CATEGORY    STRING,
PLAN_TYPE    STRING,
SOC   STRING,
TYPE    STRING,
MSF    DOUBLE,
DATA_BUCKET  DOUBLE,
DATA_BUCKET_DESC    STRING,
SUBSIDY    STRING,
PLAN_DESCRIPTION    STRING,
SALES_DESC    STRING,
FOR_SALE    INT,
FILE_NAME    STRING,
HDP_CREATED_TS TIMESTAMP
)
partitioned by (LOAD_DATE DATE)
stored as ORC;

!echo loading the table MT_PRICING.FIDO_TVM_PRICING;

INSERT OVERWRITE TABLE MT_PRICING.FIDO_TVM_PRICING PARTITION(LOAD_DATE)
SELECT
region,
plan_category,
plan_type,
soc,
type,
cast(msf as double),
cast(data_bucket as double),
data_bucket_desc,
subsidy,
plan_description,
sales_desc,
cast(for_sale as int),
trim(regexp_extract(input__file__name,'[^/]+$', 0)),
current_timestamp,
cast(substring(from_unixtime(unix_timestamp(current_timestamp,'MM/dd/yyyy')),1,10) as date) as LOAD_DATE
FROM
EXT_MT_PRICING.FIDO_TVM_PRICING;
