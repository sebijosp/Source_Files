CREATE TABLE STB_VIEWERSHIP.CBL_ROG_STB_VIEWERSHIP_SUMFCT(
Week_ID DECIMAL(4,0),
Device_Id varchar(30),
PACKAGE_TYPE varchar(50),
CHANNEL_DESC varchar(50),
DURATION_MINUTES INT

)
PARTITIONED BY (
CLNDR_MONTH INT
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
