create external table ext_stb_viewership.rpt_schedule_src_file (
CHANNEL_ID_NUMBER BIGINT,
PROGRAM_ID_NUMBER BIGINT,
PRG_DATE STRING,
PRG_TIME varchar(6),
DURATION_IN_MIN BIGINT,
DURATION BIGINT,
HD CHAR(1),
NW CHAR(1),
LIVE CHAR(1),
STEREO CHAR(1),
DVS CHAR(1),
AUDIO51 CHAR(1),
WIDE_SCREEN CHAR(1),
CC CHAR(1),
TAPED CHAR(1),
DEBUT CHAR(1),
SEASON_FINALE CHAR(1),
SEASON_PREMIERE CHAR(1),
SERIES_FINALE CHAR(1),
PRICE_PPV DECIMAL(5,2),
RPT CHAR(1),
LETTERBOX CHAR(1),
PRG_DTTM_TZ STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_schedule/landing' 
tblproperties ("skip.header.line.count"="1");
