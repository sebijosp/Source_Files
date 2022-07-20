CREATE TABLE iptv.tv_schedules (
  schemaVersion    decimal(8,2),
  content    varchar (255),
  created    date,
  copyright    varchar (255),
  start    timestamp,
  period    varchar(6),
  type    varchar (255),
  sourceId    INT,
  prgSvcId    INT,
  TMSId    varchar (255),
  time    varchar(10),
  dur    varchar (8),
  tvRating    varchar(50),
  body    varchar (100),
  partNum    INT,
  numOfParts    varchar (12),
  tvSubRating    varchar(100),
  tvsubrating_body    varchar (100),
  netSynSource    varchar (100),
  netSynType    varchar (100),
  sap    varchar (500),
  lang    varchar (100),
  subtitled    varchar(10),
  subtitled_lang    varchar(50),
  dubbed    varchar (10),
  dubbed_lang    varchar (50),
  quals    varchar (255),
  hdp_create_ts timestamp,
  hdp_update_ts timestamp
)
COMMENT 'Current data for TV Schedules'
PARTITIONED BY (AIRING_DT DATE)
Stored as ORC
