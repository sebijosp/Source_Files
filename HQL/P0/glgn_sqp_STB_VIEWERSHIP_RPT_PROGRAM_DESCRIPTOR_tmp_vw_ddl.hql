create or replace view ext_stb_viewership.rpt_program_descriptor_sqp_imp as
select
CAST(PROGRAM_ID AS BIGINT) AS PROGRAM_ID,
CAST(DESCRIPTOR_ID AS VARCHAR(8)) AS DESCRIPTOR_ID,
CAST(SRC_RANK AS BIGINT) AS SRC_RANK,
CAST(TCODE AS CHAR(1)) AS TCODE,
CAST(DELTA AS VARCHAR(3)) AS DELTA,
CAST(split(INPUT__FILE__NAME, '/userapps/glagoon/stb_viewership/rovi/rpt_program_descriptor/landing/')[1] as varchar(100)) as SOURCE_FILENAME

FROM ext_stb_viewership.rpt_program_descriptor_src_file;
