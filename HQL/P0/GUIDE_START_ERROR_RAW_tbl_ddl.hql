CREATE EXTERNAL TABLE iptv.guide_start_error_raw (
    header           STRUCT <
        `timestamp` : BIGINT,
        uuid      : STRING,
        hostname  : STRING,
        money     : STRUCT <
            traceId      : STRING,
            spanId       : BIGINT,
            parentSpanId : BIGINT,
            spanName     : STRING,
            appName      : STRING,
            startTime    : BIGINT,
            spanDuration : BIGINT,
            spanSuccess  : BOOLEAN,
            notes        : MAP<STRING, STRING>
        >
    >,
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
        dma              : STRING,
        isFlex           : BOOLEAN
    >,
    eventTimestamp   BIGINT,
    code             STRING,
    status           STRING,
    description      STRING,
    errorString      STRING,
    duration         INT,
    retryCount       INT,
    rebootCount      INT,
    lastRebootSource STRING,
    lastRebootReason STRING,
    lastRebootCustomReason STRING,
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
 '/apps/hive/warehouse/iptv.db/guide_start_error_raw'
