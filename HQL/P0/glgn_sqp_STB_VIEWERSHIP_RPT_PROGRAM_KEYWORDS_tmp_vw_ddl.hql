create or replace view ext_stb_viewership.rpt_program_keywords_sqp_imp as
select
CAST(PROGRAM_ID AS BIGINT) AS PROGRAM_ID,
CAST(KEYWORD_ID AS VARCHAR(7)) AS KEYWORD_ID,
CAST(WEIGHT AS BIGINT) AS WEIGHT,
CAST(DELTA_IND AS VARCHAR(3)) AS DELTA_IND,
CAST(split(INPUT__FILE__NAME, '/userapps/glagoon/stb_viewership/rovi/rpt_program_keywords/landing/')[1] as varchar(100)) as SOURCE_FILENAME

FROM ext_stb_viewership.rpt_program_keywords_src_file;
