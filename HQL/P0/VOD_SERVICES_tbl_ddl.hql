CREATE TABLE iptv.vod_services (
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
COMMENT 'Current data for Vod Services'
STORED AS ORC
