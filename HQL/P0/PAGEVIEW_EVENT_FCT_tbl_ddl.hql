CREATE EXTERNAL TABLE iptv.pageview_event_fct (
    header struct<
        `timestamp`    :       bigint,
        uuid           :       String,
        hostname       :       String,
        money          :       struct<
                            traceId : String,
                            spanId : bigint,
                            parentSpanId : bigint,
                            spanName : String,
                            appName : String,
                            startTime : bigint,
                            spanDuration : bigint,
                            spanSuccess : boolean,
                            notes : MAP<String, String>
       > 
    >,

    device struct<   
        receiverId      :       String,
        deviceId        :       String,
        deviceSourceId  :       String,
        account         :       String,
        accountSourceId :       String,
        billingAccountId:       String,
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
    
    eventTimestamp bigint ,
    pageName    String ,
    deepLink    String ,
    previousPageName    String ,
    appId       String ,
    hdp_file_name    String ,
    hdp_create_ts       String ,
    hdp_update_ts       String
    )
PARTITIONED BY (event_date date)
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/apps/hive/warehouse/iptv.db/pageview_event_fct'
