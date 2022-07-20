create or replace view ext_stb_viewership.rpt_schedule_sqp_imp as
select
CAST(sch.CHANNEL_ID_NUMBER AS BIGINT) AS CHANNEL_ID_NUMBER,
CAST(sch.PROGRAM_ID_NUMBER AS BIGINT) AS PROGRAM_ID_NUMBER,
CAST(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2)) AS DATE) AS PRG_DATE,
CAST(sch.PRG_TIME AS STRING) AS PRG_TIME,
CAST(sch.DURATION_IN_MIN AS BIGINT) AS DURATION_IN_MIN,
CAST(sch.DURATION AS BIGINT) AS DURATION,
CAST(sch.HD AS CHAR(1)) AS HD,
CAST(sch.NW AS CHAR(1)) AS NW,
CAST(sch.LIVE AS CHAR(1)) AS LIVE,
CAST(sch.STEREO AS CHAR(1)) AS STEREO,
CAST(sch.DVS AS CHAR(1)) AS DVS,
CAST(sch.AUDIO51 AS CHAR(1)) AS AUDIO51,
CAST(sch.WIDE_SCREEN AS CHAR(1)) AS WIDE_SCREEN,
CAST(sch.CC AS CHAR(1)) AS CC,
CAST(sch.TAPED AS CHAR(1)) AS TAPED,
CAST(sch.DEBUT AS CHAR(1)) AS DEBUT,
CAST(sch.SEASON_FINALE AS CHAR(1)) AS SEASON_FINALE,
CAST(sch.SEASON_PREMIERE AS CHAR(1)) AS SEASON_PREMIERE,
CAST(sch.SERIES_FINALE AS CHAR(1)) AS SERIES_FINALE,
CAST(sch.PRICE_PPV AS DECIMAL(5,2)) AS PRICE_PPV,
CAST(sch.RPT AS CHAR(1)) AS RPT,
CAST(sch.LETTERBOX AS CHAR(1)) AS LETTERBOX,

CAST(if((Stn.TMZNE = 'GMT' or Stn.TMZNE = 'OTH'),
from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0'))),
if(stn.TIME_ZONE_CHANGE = 'Y',
case when Stn.TMZNE = 'EST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 4 * 3600) 
 when Stn.TMZNE = 'PST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 7 * 3600)
 when Stn.TMZNE = 'ADT' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 3 * 3600)
when Stn.TMZNE = 'CST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 5 * 3600)
when Stn.TMZNE = 'MST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 6 * 3600)
when Stn.TMZNE = 'NST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 2 * 3600 - 1800)
END, 
case when Stn.TMZNE = 'EST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 5 * 3600) 
 when Stn.TMZNE = 'PST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 8 * 3600)
 when Stn.TMZNE = 'ADT' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 4 * 3600)
when Stn.TMZNE = 'CST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 6 * 3600)
when Stn.TMZNE = 'MST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 7 * 3600)
when Stn.TMZNE = 'NST' then from_unixtime(unix_timestamp(CONCAT('20', SUBSTR(sch.PRG_DATE,5,2), '-', SUBSTR(sch.PRG_DATE,1,2), '-', SUBSTR(sch.PRG_DATE,3,2), 
' ', SUBSTR(Sch.PRG_TIME,1,2),':', SUBSTR(Sch.PRG_TIME, 3, 2), ':00.0')) - 3 * 3600 - 1800)
END)) as TIMESTAMP) as PRG_DTTM_TZ
,
CAST(split(Sch.INPUT__FILE__NAME, '/userapps/glagoon/stb_viewership/rovi/rpt_schedule/landing/')[1] as varchar(100)) as SOURCE_FILENAME

FROM ext_stb_viewership.rpt_schedule_src_file sch left outer join stb_viewership.rpt_station stn on sch.channel_id_number = stn.channel_id_number;
