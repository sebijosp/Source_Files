CREATE EXTERNAL TABLE iptv.dvr_schedule_raw (
	header struct <
		`timestamp` : bigint,
    		uuid : String,
    		hostname : String,
		money : struct<
			traceId : String,
			spanId : bigint,
        		parentSpanId : bigint,
        		spanName : String,
        		appName : String,
        		startTime : bigint,
			spanDuration : bigint,
        		spanSuccess : boolean,
        		attributes : MAP<string, String>
		>
	>,
	partner String,
	eventTimestamp bigint,
	description String,

    	device struct<
		receiverId : String,
		deviceId : String,
		deviceSourceId : String,
		account : String,
		accountSourceId : String,
		macAddress : String,
		ecmMacAddress : String,
		firmwareVersion : String,
		deviceType : String,
		partner : String,
		ipAddress : String,
		utcOffset : int
		
	>,
	
	fullSchedule String,
	recordings Array< struct<
		recordingId : String,
		accountId : String,
		deviceId : String,
		title : String,
		startTime : bigint,
		duration : bigint,
		listingId : String,
		stationId : String,
		seriesId : String,
		entityId : String,
		programId : String,
		channelId : String,
		channelNumber : String,
		status : String,
		error : String
	     >
	>,
		
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
