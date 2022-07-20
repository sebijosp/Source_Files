create or replace view stb_viewership.vw_rpt_programs
as
SELECT
PROGRAM_ID_NUMBER,
LONG_TITLE,
MED_TITLE,
SHORT_TITLE,
EPISODE_TITLE,
TRUE_DURATION,
PRG_TYPE,
PRG_CATEGORY,
MPAA_RATING,
PARENTAL_RATING,
YEAR_OF_RELEASE,
STARS,
COUNTRY_OF_ORIGIN,
DIRECTOR,
ACTOR1,
ACTOR2,
ACTOR3,
ACTOR4,
ACTOR5,
ACTOR6,
ACTOR7,
ACTOR8,
ACTOR9,
ACTOR10,
ROLE1,
ROLE2,
ROLE3,
ROLE4,
ROLE5,
ROLE6,
ROLE7,
ROLE8,
ROLE9,
ROLE10,
DESCR1,
DESCR2,
DESCR3,
FULL_DESCRIPTION,
BW,
INFOMERCIAL,
VIOLENCE,
FANTASY_VIOLENCE,
LANG,
SUGGESTIVE_DIALOG,
NUDITY,
BRIEF_NUDITY,
ADULT,
SEXUAL_SITUATIONS,
EXPLICIT_SEXUAL_SITUATIONS,
SHOW_ADULT,
ALL_MALE_CAST,
ALL_FEMALE_CAST,
EPISODE_NUMBER,
FIRST_AIR_DATE,
CA_RATING,
US_RATING,
SOURCE_FILENAME,
DATALAKE_LOAD_TS
from stb_viewership.rpt_programs;
