
CREATE EXTERNAL TABLE iptv.hist_ivod_schedule (
  id	  	String,
  ivod	  	String,
  name    	String,
  title	  	String,
  exported	String,
  dateAdded	bigint,
  mediaGUID	String,
  regionIDs	 ARRAY<String>,
  requestID	String,
  startDate	bigint,
  description	String,
  stationGUID	String,
  mediaAccount	String,
  expirationDate	bigint,
  programStartTime	bigint,

  hdp_file_name String,
  hdp_create_ts String,
  hdp_update_ts String
)
PARTITIONED BY (received_date string)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
