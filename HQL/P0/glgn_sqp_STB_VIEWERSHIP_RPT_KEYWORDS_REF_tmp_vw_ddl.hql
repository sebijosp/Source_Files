create or replace view ext_stb_viewership.rpt_keywords_ref_sqp_imp as
select
CAST(KEYWORD_ID AS VARCHAR(10)) AS KEYWORD_ID,
CAST(DESCRIPTION AS VARCHAR(60)) AS DESCRIPTION,
CAST(split(INPUT__FILE__NAME, '/userapps/glagoon/stb_viewership/rovi/rpt_keywords_ref/landing/')[1] as varchar(100)) as SOURCE_FILENAME
FROM ext_stb_viewership.rpt_keywords_ref_src_file;
