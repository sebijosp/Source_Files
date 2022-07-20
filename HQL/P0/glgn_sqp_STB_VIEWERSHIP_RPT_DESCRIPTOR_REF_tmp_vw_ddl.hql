
create or replace view ext_stb_viewership.rpt_descriptor_ref_sqp_imp as
select
CAST(DESCRIPTOR_ID AS VARCHAR(7)) AS DESCRIPTOR_ID,
CAST(TCODE AS CHAR(1)) AS TCODE,
CAST(DSC AS VARCHAR(125)) AS DSC,
CAST(TEXT AS VARCHAR(8000)) AS TEXT,
CAST(split(INPUT__FILE__NAME, '/userapps/glagoon/stb_viewership/rovi/rpt_descriptor_ref/landing/')[1] as varchar(100)) as SOURCE_FILENAME
FROM ext_stb_viewership.rpt_descriptor_ref_src_file;
