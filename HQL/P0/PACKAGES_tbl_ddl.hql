CREATE TABLE iptv.packages (
  SchemaVersion  Decimal (5,2) ,
  VSPVersion  string ,
  FileName  string ,
  Response  string ,
  LinearPackageID  string ,
  IVODPackageID  string ,
  StatusCode  int ,
  StatusMessage  string ,
  StatusAttributes  string ,
  GracenoteID  string,
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Current data for Packages'
STORED AS ORC
