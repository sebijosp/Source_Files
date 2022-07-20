CREATE EXTERNAL TABLE iptv.tune_raw (
header STRUCT<
    hostname            : STRING, 
    money     : STRUCT <
            traceId      : STRING,
            spanId       : BIGINT,
            parentSpanId : BIGINT,
            spanName     : STRING,
            appName      : STRING,
            startTime    : BIGINT,
            spanDuration : BIGINT,
            spanSuccess  : BOOLEAN,
            notes        : MAP<STRING,STRING>
        >,
    `timestamp`         : BIGINT,
    uuid                : STRING
    >,
device STRUCT <
    account             : STRING, 
    accountSourceId     : STRING, 
    billingAccountId    : STRING,
    deviceId            : STRING,
    deviceSourceId      : STRING,
    deviceType          : STRING,
    dma                 : STRING,
    ecmMacAddress       : STRING,
    firmwareVersion     : STRING,
    ipAddress           : STRING, 
    isFlex              : BOOLEAN,
    macAddress          : STRING,
    make                : STRING,
    model               : STRING, 
    partner             : STRING,
    postalCode          : STRING,
    receiverId          : STRING,
    utcOffset           : BIGINT
    >,
asset STRUCT <
    assetId             : STRING, 
    channelId           : STRING, 
    companyName         : STRING, 
    externalAssetId     : STRING, 
    listingId           : STRING, 
    mediaGuid           : STRING, 
    partnerSourceId     : STRING, 
    programId           : STRING, 
    providerId          : STRING, 
    rawUrl              : STRING,
    stationId           : STRING
    >,
assetClass                      STRING 
,assetDuration                  BIGINT 
,backToXreDuration              DOUBLE 
,backToXreStart                 BIGINT 
,beginLoadDuration              DOUBLE 
,beginLoadStart                 BIGINT 
,contentType                    STRING 
,contextID                      STRING 
,deliveryMedium                 STRING 
,drmReadyDuration               DOUBLE 
,drmReadyStart                  BIGINT 
,errorCode                      STRING 
,failRetryDuration              DOUBLE 
,firstLicense                   DOUBLE 
,firstManifest                  DOUBLE 
,firstProfile                   DOUBLE 
,fragmentTotal                  DOUBLE 
,isBackupMedium                 BOOLEAN 
,isDDPlus                       BOOLEAN 
,isDemuxed                      BOOLEAN 
,isFog                          BOOLEAN 
,isPostTune                     BOOLEAN 
,isRestored                     BOOLEAN 
,isRetryTune                    BOOLEAN 
,isStartup                      BOOLEAN 
,latency                        BIGINT 
,manifestTotal                  DOUBLE 
,mediaType                      STRING 
,mimeType                       STRING 
,neededLicense                  BOOLEAN 
,networkTime                    DOUBLE 
,notifyType                     STRING 
,originalUUID                   STRING 
,playbackIndex                  BIGINT 
,playerEngine                   STRING 
,playerRequestLatency           BIGINT 
,playerResponseLatency          BIGINT 
,prepareToPlayDuration          DOUBLE 
,prepareToPlayStart             BIGINT 
,previousDeliveryMedium         STRING 
,previousPlayBackMode           STRING 
,profileCount                   STRING 
,profilesTotal                  DOUBLE 
,receiverPlatform               STRING 
,requestTimestamp               BIGINT 
,responseTimestamp              BIGINT 
,startPlay                      BIGINT 
,startPlayDuration              DOUBLE 
,startStreamingDuration         DOUBLE 
,startStreamingStart            BIGINT 
,stateStack                     STRING 
,statusMessage                  STRING 
,streamId                       STRING 
,totalTime                      DOUBLE 
,trmLatency                     BIGINT 
,tuneAttempts                   BIGINT 
,tuneStatus                     STRING 
,tuneSuccess                    BOOLEAN 
,hdp_file_name       STRING
,hdp_create_ts       STRING
,hdp_update_ts       STRING
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
 '/apps/hive/warehouse/iptv.db/tune_raw'

