create table stb_viewership.rpt_program_descriptor
(
PROGRAM_ID BIGINT,
DESCRIPTOR_ID VARCHAR(8),
SRC_RANK BIGINT,
TCODE CHAR(1),
DELTA VARCHAR(3),
SOURCE_FILENAME VARCHAR(100),
DATALAKE_LOAD_TS TIMESTAMP
)
STORED AS ORC;
