CREATE EXTERNAL TABLE iptv.tune_fct (
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
                        notes : MAP<string, String>
                >
        >,

        device struct<
                receiverId : String,
                deviceId : String,
                deviceSourceId : String,
                account : String,
                accountSourceId : String,
                billingAccountId : String,
                macAddress : String,
                ecmMacAddress : String,
                firmwareVersion : String,
                deviceType : String,
                make : String,
                model : String,
                partner : String,
                ipAddress : String,
                utcOffset : int,
                postalCode : String

        >,

        asset  struct<
          programId : String,
          rawUrl : String,
          mediaGuid : String,
          providerId : String,
          assetId : String,
          externalAssetId : String,
          stationId : String,
          channelId : String,
          partnerSourceId : String,
          listingId : String,
          companyName : String
          >,

        requestTimestamp    bigint,
        responseTimestamp    bigint,

        assetClass    String,
        deliveryMedium    String,
        tuneStatus    String,
        statusMessage    String,
        latency    int,
        isStartup    Boolean,
        notifyType    String,        

        hdp_file_name String,
        hdp_create_ts String,
        hdp_update_ts String
)
PARTITIONED BY (event_date date)
ROW FORMAT SERDE
'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
