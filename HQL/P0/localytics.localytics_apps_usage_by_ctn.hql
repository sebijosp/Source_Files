CREATE TABLE Localytics.LOCALYTICS_APPS_USAGE_BY_CTN(
RSID_WX varchar(30),
CTN varchar(10),
Hashed_CTN varchar(64),
APP_GENERIC_NAME varchar(50),
APP_PLATFORM_NAME varchar(50),
GREATEST_APP_VERSION_NO varchar(20), 
FIRST_SESSION_START_DATE timestamp,
LAST_SESSION_START_DATE timestamp,
PUSH_ENABLED_FLAG boolean,
LAST_PUSH_OPENED_DATE timestamp
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
