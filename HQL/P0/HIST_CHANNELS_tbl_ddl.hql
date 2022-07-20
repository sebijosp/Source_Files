CREATE TABLE iptv.hist_channels (
  SchemaVersion  Decimal (5,2),
  VSPVersion  string ,
  FileName  string ,
  Response  string ,
  SourceID  int ,
  GracenoteID  string ,
  x1StationID  string ,
  CallSign  string ,
  ChannelName  string ,
  Provider  string ,
  Format  string ,
  cDVREnabled  string ,
  Device_Type  string ,
  Mode_Of_Access  string ,
  StatusCode  int ,
  StatusMessage  string ,
  StatusAttributes  string,
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Historical data for Channels'
PARTITIONED BY (
  received_date date)
STORED AS ORC
