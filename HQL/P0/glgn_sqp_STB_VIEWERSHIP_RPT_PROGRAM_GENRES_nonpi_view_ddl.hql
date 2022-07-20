create or replace view stb_viewership_nonpi.vw_rpt_program_genres
as
select
PROGRAM_ID,
GENRE,
GENRE_SEQUENCE,
DELTA_IND,
PROGRAM_GENRE_ID,
GENRE_ID,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_program_genres;
