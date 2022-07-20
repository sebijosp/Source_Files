CREATE TABLE iptv.hist_packages (
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
COMMENT 'Historical data for Packages'
PARTITIONED BY (
  received_date date)
STORED AS ORC
