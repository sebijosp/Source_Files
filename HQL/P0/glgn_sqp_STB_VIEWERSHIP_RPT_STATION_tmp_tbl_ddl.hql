create external table ext_stb_viewership.rpt_station_src_file (
CHANNEL_ID_NUMBER BIGINT,
CALL_LETTER VARCHAR(50),
EXPANDED_CALL_LETTER VARCHAR(50),
TMZNE VARCHAR(3),
TIME_ZONE_CHANGE CHAR(1),
CITY VARCHAR(40),
STATE VARCHAR(50),
WEBSITE_ADDRESS VARCHAR(254),
NETWORK VARCHAR(10))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_station/landing'
tblproperties ("skip.header.line.count"="1");
