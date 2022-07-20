CREATE TABLE iptv.hist_vod_packages (
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
COMMENT 'Historical data for Vod Packages'
PARTITIONED BY (received_date date)
