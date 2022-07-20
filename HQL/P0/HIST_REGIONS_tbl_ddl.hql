CREATE TABLE iptv.hist_regions (
  SchemaVersion  Decimal (5,2),
  VSPVersion  String ,
  FileName  string ,
  Response String ,
  ContentRegionID  string,
  Type  string,
  StatusCode  INT ,
  StatusMessage  String ,
  StatusAttributes  String,
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Historical data for Regions'
PARTITIONED BY (
  received_date date)
STORED as ORC
