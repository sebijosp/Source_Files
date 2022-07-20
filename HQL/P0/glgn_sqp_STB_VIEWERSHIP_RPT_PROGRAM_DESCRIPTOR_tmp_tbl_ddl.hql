create external table ext_stb_viewership.rpt_program_descriptor_src_file (
PROGRAM_ID BIGINT,
DESCRIPTOR_ID VARCHAR(8),
SRC_RANK BIGINT,
TCODE CHAR(1),
DELTA VARCHAR(3))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_program_descriptor/landing' ;
