CREATE EXTERNAL TABLE iptv.search_message_raw (
    header struct<
        `timestamp`    :  bigint,
        uuId           :  String,
        hostname       :  String,
        money          :  struct<
                            traceId      : String,
                            spanId       : bigint,
                            parentSpanId : bigint,
                            spanName     : String,
                            appName      : String,
                            startTime    : bigint,
                            spanDuration : bigint,
                            spanSuccess  : boolean,
                            notes        : MAP<String, String>
                        >
    >,  
    device struct<   
        receiverId      :       String,
        deviceId        :       String,
        deviceSourceId  :       String,
        account         :       String,
        accountSourceId :       String,
        billingaccountId:       String,
        macAddress      :       String,
        ecmMacAddress   :       String,
        firmwareVersion :       String,
        deviceType      :       String,
        make            :       String,
        model           :       String,
        partner         :       String,
        ipAddress       :       String,
        utcOffset       :       int,
        postalCode      :       String,
        dma             :       String
    >,
    eventTimestamp      bigint,
    status              String,
    statusMessage       String,
    resultCount         int,
    searchTerm          String,
    isT9                boolean,
    channelId           String,
    searchTime          int,
    position            int,
    programId           String,
    searchEntityType    String,
    trackingMetricType  String,
    hdp_file_name       String,
    hdp_create_ts       String,
    hdp_update_ts       String
    )
PARTITIONED BY (received_date date)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/apps/hive/warehouse/iptv.db/search_message_raw'

