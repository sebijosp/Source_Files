set hive.execution.engine=tez;

!echo Parms to Hive Script: filename='${hiveconf:file_name}';

create external table if not exists EXT_DATA_PODS.IUM_CATEGORY_LOOKUP(
SMS_NOTIFICTIONID      STRING,
DESCRIPTION             STRING,
IUM_75_IND              STRING,
IUM_90_IND              STRING,
IUM_100_IND             STRING,
IUM_SUSPEND_IND         STRING,
BRAND                   STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' =',')
STORED AS TEXTFILE
LOCATION '${hiveconf:file_name}'
tblproperties("skip.header.line.count"="1");

create table if not exists data_pods.DIM_WL_IUM_SUB_DLY
(
SMS_NOTIFICTIONID      varchar(100),
DESCRIPTION             varchar(100),
IUM_75_IND              SMALLINT,
IUM_90_IND              SMALLINT,
IUM_100_IND             SMALLINT,
IUM_SUSPEND_IND         SMALLINT,
BRAND                   varchar(20),
FILE_NAME varchar(100),
HDP_CREATED_TS TIMESTAMP
)
stored as ORC;

!echo loading the table data_pods.DIM_WL_IUM_SUB_DLY;

INSERT OVERWRITE TABLE data_pods.DIM_WL_IUM_SUB_DLY
SELECT
SMS_NOTIFICTIONID,
DESCRIPTION,
cast(IUM_75_IND as smallint),
cast(IUM_90_IND as smallint),
cast(IUM_100_IND as smallint),
cast(IUM_SUSPEND_IND as smallint),
BRAND,
trim(regexp_extract(input__file__name,'[^/]+$', 0)),
current_timestamp
FROM
EXT_DATA_PODS.IUM_CATEGORY_LOOKUP;
