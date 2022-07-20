CREATE EXTERNAL TABLE iptv.ip_playback_reprocess_raw (
    header              STRUCT <
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
            notes        :  MAP<STRING,STRING>
        >
    >,
    partnerId           STRING,
    session             STRUCT <
        pluginSessionId : STRING,
        playbackId      : INT
    >,
    startTime           BIGINT,
    eventType           STRING,
    completionStatus    STRING,
    sessionDuration     INT,
    device              STRUCT <
        physicalDeviceId : STRING,
        serviceAccountId : STRING,
        deviceType       : STRING,
        deviceName       : STRING,
        deviceVersion    : STRING,
        ipAddress        : STRING,
        networkLocation  : STRING,
        deviceSourceId   : STRING,
        accountSourceId  : STRING,
        customFields     : map<string,string>
    >,
    application         STRUCT <
        applicationName    : STRING,
        applicationVersion : STRING,
        playerName         : STRING,
        playerVersion      : STRING,
        pluginName         : STRING,
        pluginVersion      : STRING,
        userAgent          : STRING,
        customFields       : map<string,string>
    >,
    asset               STRUCT <
        assetClass      : STRING,
        regulatoryClass : STRING,
        playbackType    : STRING,
        mediaGuid       : STRING,
        providerId      : STRING,
        assetId         : STRING,
        assetContentId  : STRING,
        mediaId         : STRING,
        platformId      : STRING,
        recordingId     : STRING,
        streamId        : STRING,
        easUri          : STRING,
        manifestUrl     : STRING,
        virtualStream   : STRUCT <
            timing      : STRUCT <
                clientGeneratedTimestamp : BIGINT,
                clientPostTimestamp      : BIGINT,
                serverReceivedTimestamp  : BIGINT,
                clientTimestamp          : BIGINT,
                position                 : INT
            >,
            eventName    : STRING,
            sourceId     : STRING,
            signalId     : STRING,
            serviceZone  : STRING
        >,
        title            : STRING,
        customFields     : map<string,string>
    >,
    geolocation         STRUCT <
        city       : STRING,
        region     : STRING,
        postalCode : STRING,
        country    : STRING,
        latitude   : DOUBLE,
        longitude  : DOUBLE,
        dma        : STRING,
        utcOffset  : FLOAT
    >,
    metrics             STRUCT <
        bufferUnderflow     : BOOLEAN,
        bufferTime          : BIGINT,
        bufferRatio         : STRING,
        bufferCount         : INT,
        fatalError          : BOOLEAN,
        errorCount          : INT,
        mediaOpenLatency    : BIGINT,
        avgBitRate          : STRING,
        maxHeartBeatBitrate : BIGINT
    >,
       virtualStream      array<STRING>,
    heartBeat          array<struct<
        timing :STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT>,
        bitrate:INT,
        framerate:INT,
        secondaryAudio:STRING,
        closedCaptions:STRING,
        playState:STRING,
        bufferSize:INT,
        bufferLength:INT,
        fragmentCount:INT,
        fragmentSize:INT,
        fragmentDuration:INT,
        fragmentDownloadLatency:INT,
        fragmentDownloadDuration:INT
    >>,

    pluginInitialized          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        screenWidth:INT,screenHeight:INT
    >>,
   
    openingMedia          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        retry:INT
    >>,
  
    mediaOpened          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        latency:INT,
        paused:BOOLEAN
    >>,
   
    playbackStarted          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
    >>>,
    
    mediaFailed              array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        errorCode:STRING,
        adError:BOOLEAN,
        errorDescription:STRING
    >>,
   
    bitrateChanged          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        bitrate:INT
    >>,
   
    adProgress          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
    value:INT,
    advertisement:STRING
    >>,
    
    fragmentWarning          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        fragmentUrl:STRING,
        fragmentSize:INT,
        fragmentDuration:INT,
        fragmentDownloadLatency:INT,
        fragmentDownloadDuration:INT
    >>,
   
    error              array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,position:INT
        >,
        errorCode:STRING,
        adError:BOOLEAN,
        errorDescription:STRING
    >>,
   
    eas              array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        uri:STRING,
        action:STRING,
        language:STRING,
        errorCode:STRING
    >>,
   
    trickPlay              array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        trickPlayType:STRING,
        playRate:INT
    >>,
   
    bufferEvent              array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        bufferEventType:STRING,
        state:STRING,
        startTime:BIGINT,
        duringAd:BOOLEAN
    >>,
   
    playStateChanged          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
       `value`:STRING
    >>,
   
    mediaEnded          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        reason:STRING
    >>,
   
    licenseAcquired          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        latency:INT
    >>,
    
    seek          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        `type`:STRING
    >>,
  
    genericEvent          array<struct<
        timing:STRUCT<
            clientGeneratedTimestamp:BIGINT,
            clientPostTimestamp:BIGINT,
            serverReceivedTimestamp:BIGINT,
            clientTimestamp:BIGINT,
            position:INT
        >,
        context: map<STRING,STRING>,
        eventName:STRING
    >>,
    hdp_file_name       STRING,
    hdp_create_ts       STRING,
    hdp_update_ts       STRING
)
PARTITIONED BY (received_date string)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
 '/apps/hive/warehouse/iptv.db/ip_playback_reprocess_raw'
;

