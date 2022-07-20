create or replace view stb_viewership_nonpi.vw_rpt_descriptor_all_language_ref
as
select
DESCRIPTOR_ID,
LANGUAGE_ID,
TCODE,
DSC,
TEXT,
DELTA_IND,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_descriptor_all_language_ref;
