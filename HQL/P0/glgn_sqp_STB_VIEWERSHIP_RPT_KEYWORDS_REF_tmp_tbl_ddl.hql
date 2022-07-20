create external table ext_stb_viewership.rpt_keywords_ref_src_file (
KEYWORD_ID VARCHAR(10),
DESCRIPTION VARCHAR(60))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_keywords_ref/landing' ;
