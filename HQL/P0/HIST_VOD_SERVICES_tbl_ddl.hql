CREATE TABLE iptv.hist_vod_services (
  SchemaVersion  Decimal (5,2) ,
  VSPVersion  String ,
  FileName  string ,
  Response String ,
  VODServiceID  String ,
  Studio  String ,
  StatusCode  string ,
  StatusMessage  string ,
  StatusAttributes  string,
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Historical data for Vod Services'
PARTITIONED BY (received_date date)
STORED AS ORC
