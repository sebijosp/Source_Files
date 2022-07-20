CREATE TABLE iptv.vod_packages (
  SchemaVersion  Decimal (5,2) ,
  VSPVersion  string ,
  FileName  string ,
  Response  string ,
  VODPackageID  string ,
  StatusCode  int ,
  StatusMessage  string ,
  StatusAttributes  string ,
  VODServiceID  string,
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Current data for Vod Packages'
STORED AS ORC
