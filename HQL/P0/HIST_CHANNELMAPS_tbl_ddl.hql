CREATE TABLE iptv.hist_channelmaps (
  SchemaVersion  Decimal(5,2) ,
  VSPVersion  string ,
  FileName  string ,
  Response  string ,
  ChannelMapIdTitleVI  string ,
  ChannelMapIdTVE  string ,
  LinearRegionID  string ,
  VODRegionID  string ,
  StatusCode  int ,
  StatusMessage  string ,
  StatusAttributes  string ,
  GracenoteID  string ,
  VirtualChannel  int,
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Historical data for Channel Maps'
PARTITIONED BY (
  received_date date)
