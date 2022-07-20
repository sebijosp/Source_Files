set hive.execution.engine=tez;

!echo Parms to Hive Script: filename='${hiveconf:file_name}';

create external table if not exists ext_cdrdm.SPM_WCOC_CONSENT_DETAILS(
EVENT_TIME 	STRING,
NME_CLASS	STRING,
NME_TYPE	STRING,
CTN	STRING,
BAN	STRING,
SUBSCRIBER_TYPE	STRING,
TX_TYPE	STRING,
SESSION_ID	STRING,
RCONSENT STRING,
DCONSENT STRING,
RESULT STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' ='\,')
STORED AS TEXTFILE
LOCATION '${hiveconf:file_name}'
tblproperties("skip.header.line.count"="1");

create table if not exists CDRDM.SPM_WCOC_CONSENT_DETAILS
(
EVENT_TIME      STRING,
NME_CLASS       STRING,
NME_TYPE        STRING,
CTN     STRING,
BAN     STRING,
SUBSCRIBER_TYPE STRING,
TX_TYPE STRING,
SESSION_ID      STRING,
RCONSENT STRING,
DCONSENT STRING,
RESULT STRING,
FILE_NAME    STRING,
HDP_CREATED_TS TIMESTAMP
)
partitioned by (EVENT_DATE DATE)
stored as ORC;

!echo loading the table CDRDM.SPM_WCOC_CONSENT_DETAILS;

INSERT INTO TABLE CDRDM.SPM_WCOC_CONSENT_DETAILS PARTITION(EVENT_DATE)
SELECT
cast(from_unixtime(CAST(EVENT_TIME as BIGINT), 'yyyy-MM-dd HH:mm:ss') as timestamp),
NME_CLASS, 
NME_TYPE,  
CTN, 
BAN, 
SUBSCRIBER_TYPE, 
TX_TYPE,
SESSION_ID,
RCONSENT,
DCONSENT,
RESULT,
trim(regexp_extract(input__file__name,'[^/]+$', 0)),
current_timestamp,
cast(from_unixtime(CAST(EVENT_TIME as BIGINT), 'yyyy-MM-dd') as date) as EVENT_DATE
FROM
ext_cdrdm.SPM_WCOC_CONSENT_DETAILS;
