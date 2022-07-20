create table ext_stb_viewership.hist_rpt_keywords_ref_sqp_imp
(
KEYWORD_ID VARCHAR(10),
DESCRIPTION VARCHAR(60),
source_filename varchar(100),
DATALAKE_LOAD_TS TIMESTAMP
)
STORED AS ORC;
