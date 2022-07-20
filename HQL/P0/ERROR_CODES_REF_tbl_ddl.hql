CREATE TABLE IPTV.ERROR_CODES_REF(
  COLUMN_NAME            STRING,
   ERROR_DESCRIPTION      STRING,
   ERROR_CODE             STRING,
   SOURCE_SYSTEM          STRING,
   SOURCE_FIELD           STRING,
   COMMENTS               STRING,
   TECHNICAL_OWNER        STRING,
   BUSINESS_OWNER         STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;
