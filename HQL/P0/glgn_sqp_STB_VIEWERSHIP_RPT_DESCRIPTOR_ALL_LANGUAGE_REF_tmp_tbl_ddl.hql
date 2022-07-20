create external table ext_stb_viewership.rpt_descriptor_all_language_ref_src_file (
DESCRIPTOR_ID VARCHAR(7),
LANGUAGE_ID VARCHAR(2),
TCODE CHAR(1),
DSC VARCHAR(125),
TEXT VARCHAR(8000),
DELTA_IND VARCHAR(3))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_descriptor_ref_all_language/landing' ;
