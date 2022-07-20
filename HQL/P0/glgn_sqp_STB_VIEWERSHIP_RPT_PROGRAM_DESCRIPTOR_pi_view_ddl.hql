create or replace view stb_viewership.vw_rpt_program_descriptor
as
select
PROGRAM_ID,
DESCRIPTOR_ID,
SRC_RANK,
TCODE,
DELTA,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_program_descriptor;
