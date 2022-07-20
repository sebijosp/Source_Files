create or replace view stb_viewership.vw_rpt_keywords_ref
as
select
KEYWORD_ID,
DESCRIPTION,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_keywords_ref;
