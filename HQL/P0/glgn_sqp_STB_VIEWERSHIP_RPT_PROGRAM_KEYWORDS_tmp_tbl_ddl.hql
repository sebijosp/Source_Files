create external table ext_stb_viewership.rpt_program_keywords_src_file (
PROGRAM_ID BIGINT,
KEYWORD_ID VARCHAR(7),
WEIGHT BIGINT,
DELTA_IND VARCHAR(3))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_program_keywords/landing' ;
