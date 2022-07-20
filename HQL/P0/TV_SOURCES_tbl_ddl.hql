CREATE TABLE iptv.tv_sources (
  schemaVersion    decimal(8,2),
  content    VARCHAR(255),
  created    date,
  copyright    VARCHAR(255),
  start    timestamp,
  period    VARCHAR(6),
  sourceId    int,
  prgSvcId    int,
  name    VARCHAR(60),
  city    VARCHAR(50),
  state    VARCHAR(50),
  postalCode    VARCHAR(15),
  country    VARCHAR(3),
  prgsvc_type    VARCHAR(30),
  relationship    VARCHAR(200),
  relationship_type    VARCHAR(200),
  attrib    VARCHAR(50),
  timeZone    VARCHAR(100),
  callSign    VARCHAR(10),
  edLang    VARCHAR(60),
  bcastLang    VARCHAR(100),
  num    int,
  majorNum    int,
  minorNum    int,
  affil    VARCHAR(60),
  afffil_prgsvcid    int,
  URL    VARCHAR(100),
  marketId    int,
  marketid_type    VARCHAR(255),
  image_type    VARCHAR(100),
  width    varchar(100),
  height    varchar(100),
  primary    varchar(100),
  category    VARCHAR(300),
  URI    VARCHAR(500),
  creditLine    VARCHAR(255),
  hdp_create_ts timestamp,
  hdp_update_ts timestamp

)
COMMENT 'Current data for TV Sources'

STORED AS ORC
