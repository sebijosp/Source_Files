
create external table ext_stb_viewership.rpt_descriptor_ref_src_file (
DESCRIPTOR_ID  string,
TCODE  string,
DSC  string,
TEXT  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_descriptor_ref/landing';

