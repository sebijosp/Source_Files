CREATE TABLE iptv.hist_tv_sources (
  schemaVersion    String,
  content    String,
  created    String,
  copyright    String,
  start    String,
  period    String,
  sourceId    String,
  prgSvcId    String,
  name    String,
  city    String,
  state    String,
  postalCode    String,
  country    String,
  prgsvc_type    String,
  relationship    String,
  relationship_type    String,
  attrib    String,
  timeZone    String,
  callSign    String,
  edLang    String,
  bcastLang    String,
  num    String,
  majorNum    String,
  minorNum    String,
  affil    String,
  afffil_prgsvcid    String,
  URL    String,
  marketId    String,
  marketid_type    String,
  image_type    String,
  width    String,
  height    String,
  primary    String,
  category    String,
  URI    String,
  creditLine    String,
  source_filename String,
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Hist data for TV Sources'
PARTITIONED BY (received_date date)
STORED as ORC
