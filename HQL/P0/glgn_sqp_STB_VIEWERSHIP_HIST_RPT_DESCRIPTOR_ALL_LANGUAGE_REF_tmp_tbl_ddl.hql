create table ext_stb_viewership.hist_rpt_descriptor_all_language_ref_sqp_imp
(
DESCRIPTOR_ID VARCHAR(7),
LANGUAGE_ID VARCHAR(2),
TCODE CHAR(1),
DSC VARCHAR(125),
TEXT VARCHAR(8000),
DELTA_IND VARCHAR(3),
SOURCE_FILENAME VARCHAR(100),
DATALAKE_LOAD_TS TIMESTAMP
)
STORED AS ORC;
