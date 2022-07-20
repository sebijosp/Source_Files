create external table ext_stb_viewership.rpt_program_genres_src_file (
PROGRAM_ID BIGINT,
GENRE VARCHAR(60),
GENRE_SEQUENCE BIGINT,
DELTA_IND VARCHAR(3),
PROGRAM_GENRE_ID BIGINT,
GENRE_ID BIGINT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_program_genres/landing' ;
