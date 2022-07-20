create or replace view ext_stb_viewership.rpt_program_genres_sqp_imp as
select
CAST(PROGRAM_ID AS BIGINT) AS PROGRAM_ID,
CAST(GENRE AS VARCHAR(60)) AS GENRE,
CAST(GENRE_SEQUENCE AS BIGINT) AS GENRE_SEQUENCE,
CAST(DELTA_IND AS VARCHAR(3)) AS DELTA_IND,
CAST(PROGRAM_GENRE_ID AS BIGINT) AS PROGRAM_GENRE_ID,
CAST(GENRE_ID AS BIGINT) AS GENRE_ID,
CAST(split(INPUT__FILE__NAME, '/userapps/glagoon/stb_viewership/rovi/rpt_program_genres/landing/')[1] as varchar(100)) as SOURCE_FILENAME
FROM ext_stb_viewership.rpt_program_genres_src_file;
