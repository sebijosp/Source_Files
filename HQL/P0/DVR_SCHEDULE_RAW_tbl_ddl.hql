CREATE EXTERNAL TABLE iptv.dvr_schedule_raw (
    header           STRUCT <
        `timestamp` : BIGINT,
        uuid        : STRING,
        hostname    : STRING,
        money       : STRUCT <
            traceId      : STRING,
            spanId       : BIGINT,
            parentSpanId : BIGINT,
            spanName     : STRING,
            appName      : STRING,
            startTime    : BIGINT,
            spanDuration : BIGINT,
            spanSuccess  : BOOLEAN,
            notes        : MAP <STRING, STRING>
        >
    >,
    partner          STRING,
    eventTimestamp   BIGINT,
    description      STRING,
    device           STRUCT <
        receiverId       : STRING,
        deviceId         : STRING,
        deviceSourceId   : STRING,
        account          : STRING,
        accountSourceId  : STRING,
        billingAccountId : STRING,
        macAddress       : STRING,
        ecmMacAddress    : STRING,
        firmwareVersion  : STRING,
        deviceType       : STRING,
        make             : STRING,
        model            : STRING,
        partner          : STRING,
        ipAddress        : STRING,
        utcOffset        : INT,
        postalCode       : STRING,
        dma              : STRING
    >,
    fullSchedule     STRING,
    recordings       ARRAY <  STRUCT < 
        recordingId      : STRING,
        accountId        : STRING,
        accountSourceId  : STRING,
        deviceId         : STRING,
        deviceSourceId   : STRING,
        title            : STRING,
        cloudRecording   : STRING,
        startTime        : BIGINT,
        duration         : BIGINT,
        listingId        : STRING,
        stationId        : STRING,
        seriesId         : STRING,
        entityId         : STRING,
        programId        : STRING,
        channelId        : STRING,
        channelNumber    : STRING,
        status           : STRING,
        error            : STRING
		>
    >,
    hdp_file_name    STRING,
    hdp_create_ts    STRING,
    hdp_update_ts    STRING
)
PARTITIONED BY (received_date DATE) 
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
 '/apps/hive/warehouse/iptv.db/dvr_schedule_raw'
