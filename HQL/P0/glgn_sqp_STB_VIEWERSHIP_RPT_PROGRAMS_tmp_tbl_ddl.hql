create external table ext_stb_viewership.rpt_programs_src_file (
PROGRAM_ID_NUMBER BIGINT,
LONG_TITLE VARCHAR(100),
MED_TITLE VARCHAR(100),
SHORT_TITLE VARCHAR(100),
EPISODE_TITLE VARCHAR(200),
TRUE_DURATION INT,
PRG_TYPE VARCHAR(20),
PRG_CATEGORY VARCHAR(40),
MPAA_RATING VARCHAR(20),
PARENTAL_RATING VARCHAR(20),
YEAR_OF_RELEASE INT,
STARS INT,
COUNTRY_OF_ORIGIN VARCHAR(100),
DIRECTOR VARCHAR(50),
ACTOR1 VARCHAR(100),
ACTOR2 VARCHAR(100),
ACTOR3 VARCHAR(100),
ACTOR4 VARCHAR(100),
ACTOR5 VARCHAR(100),
ACTOR6 VARCHAR(100),
ACTOR7 VARCHAR(100),
ACTOR8 VARCHAR(100),
ACTOR9 VARCHAR(100),
ACTOR10 VARCHAR(100),
ROLE1 VARCHAR(100),
ROLE2 VARCHAR(100),
ROLE3 VARCHAR(100),
ROLE4 VARCHAR(100),
ROLE5 VARCHAR(100),
ROLE6 VARCHAR(100),
ROLE7 VARCHAR(100),
ROLE8 VARCHAR(100),
ROLE9 VARCHAR(100),
ROLE10 VARCHAR(100),
DESCR1 VARCHAR(1000),
DESCR2 VARCHAR(1000),
DESCR3 VARCHAR(1000),
FULL_DESCRIPTION VARCHAR(5000),
BW CHAR(1),
INFOMERCIAL CHAR(1),
VIOLENCE CHAR(1),
FANTASY_VIOLENCE CHAR(1),
LANG CHAR(1),
SUGGESTIVE_DIALOG CHAR(1),
NUDITY CHAR(1),
BRIEF_NUDITY CHAR(1),
ADULT CHAR(1),
SEXUAL_SITUATIONS CHAR(1),
EXPLICIT_SEXUAL_SITUATIONS CHAR(1),
SHOW_ADULT CHAR(1),
ALL_MALE_CAST CHAR(1),
ALL_FEMALE_CAST CHAR(1),
EPISODE_NUMBER BIGINT,
FIRST_AIR_DATE STRING,
CA_RATING VARCHAR(15),
US_RATING VARCHAR(15))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "\|",
"quoteChar"     = "\""
)
STORED AS TEXTFile
LOCATION '/userapps/glagoon/stb_viewership/rovi/rpt_programs/landing' 
tblproperties ("skip.header.line.count"="1");
