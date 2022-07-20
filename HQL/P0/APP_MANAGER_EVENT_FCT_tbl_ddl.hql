CREATE TABLE iptv.app_manager_event_fct (
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
            notes        : MAP<String, String>
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
    appId            STRING,
    launchedFrom     STRING,
    pinChallenged    BOOLEAN,
    description      STRING,
    state            STRING,
    reason           STRING,
    hdp_file_name    STRING,
    hdp_create_ts    STRING,
    hdp_update_ts    STRING
)
PARTITIONED BY (event_date DATE) 
STORED AS ORC;
