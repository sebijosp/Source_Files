create or replace view stb_viewership_nonpi.vw_rpt_station
as
select
CHANNEL_ID_NUMBER,
CALL_LETTER,
EXPANDED_CALL_LETTER,
TMZNE,
TIME_ZONE_CHANGE,
CITY,
STATE,
WEBSITE_ADDRESS,
NETWORK,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_station;
