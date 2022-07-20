create or replace view stb_viewership_nonpi.vw_rpt_program_keywords
as
select
PROGRAM_ID,
KEYWORD_ID,
WEIGHT,
DELTA_IND,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_program_keywords;
