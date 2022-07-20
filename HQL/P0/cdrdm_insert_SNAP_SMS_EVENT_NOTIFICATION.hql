set hive.execution.engine=tez;

!echo Parms to Hive Script: filename='${hiveconf:file_name}';

create external table if not exists ext_cdrdm.SNAP_SMS_EVENT_NOTIFICATION(
EVENT_TIME	STRING,
NME_CLASS	STRING,
NME_TYPE	STRING,
CTN	STRING,
BAN	STRING,
SUBSCRIBER_TYPE	STRING,
TX_TYPE	STRING,
SESSION_ID	STRING,
SPID	STRING,
LANGUAGE	STRING,
SERVICETYPE	STRING,
SHORTCODE	STRING,
NOTIFICATIONTEXT	STRING,
ESMCLASS	STRING,
PROTOCOL_ID	STRING,
DATA_CODING	STRING,
MSG_REF_NUMBER	STRING,
THRESHOLD_LEVEL	STRING,
MONEY_USAGE	STRING,
MSG_TYPE	STRING,
BILLCYCLE_STARTDATE	STRING,
BILLCYCLE_ENDDATE	STRING,
JMS_EVENTTYPE	STRING,
JMS_TRANSACTIONCODE	STRING,
SUBMIT_STATUS	STRING,
NOTIF_DATE	STRING,
JMS_SUBMITSTATUS	STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' ='\,')
STORED AS TEXTFILE
LOCATION '${hiveconf:file_name}'
tblproperties("skip.header.line.count"="1");

create table if not exists CDRDM.SNAP_SMS_EVENT_NOTIFICATION
(
EVENT_TIME	STRING,
NME_CLASS	STRING,
NME_TYPE	STRING,
CTN	STRING,
BAN	STRING,
SUBSCRIBER_TYPE	STRING,
TX_TYPE	STRING,
SESSION_ID	STRING,
SPID	STRING,
LANGUAGE	STRING,
SERVICETYPE	STRING,
SHORTCODE	STRING,
NOTIFICATIONTEXT	STRING,
ESMCLASS	STRING,
PROTOCOL_ID	STRING,
DATA_CODING	STRING,
MSG_REF_NUMBER	STRING,
THRESHOLD_LEVEL	STRING,
MONEY_USAGE	STRING,
MSG_TYPE	STRING,
BILLCYCLE_STARTDATE	STRING,
BILLCYCLE_ENDDATE	STRING,
JMS_EVENTTYPE	STRING,
JMS_TRANSACTIONCODE	STRING,
SUBMIT_STATUS	STRING,
NOTIF_DATE	STRING,
JMS_SUBMITSTATUS	STRING,
FILE_NAME    STRING,
HDP_CREATED_TS TIMESTAMP
)
partitioned by (EVENT_DATE DATE)
stored as ORC;

!echo loading the table CDRDM.SNAP_SMS_EVENT_NOTIFICATION;

INSERT INTO TABLE CDRDM.SNAP_SMS_EVENT_NOTIFICATION PARTITION(EVENT_DATE)
SELECT
EVENT_TIME,
NME_CLASS,
NME_TYPE,
CTN,
BAN,
SUBSCRIBER_TYPE,
TX_TYPE,
SESSION_ID,
SPID,
LANGUAGE,
SERVICETYPE,
SHORTCODE,
NOTIFICATIONTEXT,
ESMCLASS,
PROTOCOL_ID,
DATA_CODING,
MSG_REF_NUMBER,
THRESHOLD_LEVEL,
MONEY_USAGE,
MSG_TYPE,
BILLCYCLE_STARTDATE,
BILLCYCLE_ENDDATE,
JMS_EVENTTYPE,
JMS_TRANSACTIONCODE,
SUBMIT_STATUS,
NOTIF_DATE,
JMS_SUBMITSTATUS,
trim(regexp_extract(input__file__name,'[^/]+$', 0)),
current_timestamp,
cast(substring(from_unixtime(unix_timestamp(EVENT_TIME,'MM/dd/yyyy')),1,10) as date) as EVENT_DATE
FROM
ext_cdrdm.SNAP_SMS_EVENT_NOTIFICATION;
