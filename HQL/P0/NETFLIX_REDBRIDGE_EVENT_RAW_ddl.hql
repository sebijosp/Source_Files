CREATE EXTERNAL TABLE iptv.netflix_redbridge_raw (
    header                STRUCT <
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
    eventType             STRING,
    eventSource           STRING,
    provider              STRING,
    serviceName           STRING,
    eventTimestamp        BIGINT,
    partner               STRING,
    transactionID         STRING,
    accountID             STRING,
    xboServiceAccountId   STRING,
    paid                  STRING,
    upsellHold            BOOLEAN,
    transactionalHold     BOOLEAN,
    denyCharge            BOOLEAN,
    pinRequired           BOOLEAN,
    action                STRING,
    status                STRING,
    responseMessages      STRING,
    fromAccount           STRING,
    toAccount             STRING,
    chargeAmount          FLOAT,
    currency              STRING,
    serviceActivated      STRING,
    planType              STRING,
    endDate               STRING,
    description           STRING,
    esn                   STRING,
    hdp_file_name         STRING,
    hdp_create_ts         STRING,
    hdp_update_ts         STRING
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
 '/apps/hive/warehouse/iptv.db/netflix_redbridge_raw'
