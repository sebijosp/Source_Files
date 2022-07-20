create or replace view stb_viewership_nonpi.vw_rpt_descriptor_ref
as
select
DESCRIPTOR_ID,
TCODE,
DSC,
TEXT,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_descriptor_ref;
