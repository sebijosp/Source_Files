CREATE TABLE LOCALYTICS.LOCALYTICS_DEVICES_BY_CTN(
RSID_WX varchar(30),
CTN varchar(10),
HASHED_CTN varchar(64),
ASSOCIATION_APP_NAME varchar(50),
DEVICE_PLATFORM varchar(20),
DEVICE_MAKE varchar(50),
DEVICE_MODEL varchar(100),
MOBILE_AD_ID varchar(100),
LAST_DEVICE_SESSION_DATE timestamp
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
	STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
	OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
