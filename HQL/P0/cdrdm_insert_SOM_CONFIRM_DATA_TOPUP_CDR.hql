set hive.execution.engine=tez;

!echo Parms to Hive Script: filename='${hiveconf:file_name}';

create external table if not exists ext_cdrdm.SOM_CONFIRM_DATA_TOPUP_CDR(
EVENT_TIME      STRING,
NME_CLASS       STRING,
NME_TYPE        STRING,
CTN     STRING,
BAN     STRING,
SUBSCRIBER_TYPE STRING,
TX_TYPE STRING,
SESSION_ID      STRING,
SOCCODE  STRING,
RESULTCODE  STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' ='\,')
STORED AS TEXTFILE
LOCATION '${hiveconf:file_name}'
tblproperties("skip.header.line.count"="1");

create table if not exists CDRDM.SOM_CONFIRM_DATA_TOPUP_CDR
(
EVENT_TIME      STRING,
NME_CLASS       STRING,
NME_TYPE        STRING,
CTN     STRING,
BAN     STRING,
SUBSCRIBER_TYPE STRING,
TX_TYPE STRING,
SESSION_ID      STRING,
SOCCODE  STRING,
RESULTCODE  STRING,
FILE_NAME    STRING,
HDP_CREATED_TS TIMESTAMP
)
partitioned by (EVENT_DATE DATE)
stored as ORC;

!echo loading the table CDRDM.SOM_CONFIRM_DATA_TOPUP_CDR;

INSERT INTO TABLE CDRDM.SOM_CONFIRM_DATA_TOPUP_CDR PARTITION(EVENT_DATE)
SELECT
EVENT_TIME,
NME_CLASS,
NME_TYPE,
CTN,
BAN,
SUBSCRIBER_TYPE,
TX_TYPE,
SESSION_ID,
SOCCODE,
RESULTCODE,
trim(regexp_extract(input__file__name,'[^/]+$', 0)),
current_timestamp,
cast(substring(from_unixtime(unix_timestamp(EVENT_TIME,'MM/dd/yyyy')),1,10) as date) as EVENT_DATE
FROM
ext_cdrdm.SOM_CONFIRM_DATA_TOPUP_CDR;
