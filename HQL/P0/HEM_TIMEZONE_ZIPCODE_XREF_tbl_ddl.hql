DROP TABLE IF EXISTS HEM.TIMEZONE_ZIPCODE_XREF;
CREATE TABLE HEM.TIMEZONE_ZIPCODE_XREF(
   CITY             VARCHAR(50)  COMMENT "City Name associated with the zipcode",
   PROVINCE         VARCHAR(2)   COMMENT "2 character abbreviation for Province",
   ZIPCODE          VARCHAR(7)   COMMENT "6 characte zipcode with embeded space in format ANA NAN",
   PREFFERED        CHAR(1)      COMMENT "P - Preferred city name for zipcode. A - Approved (not Preffered) city name for zipcode.",
   AREA_CODE        INT          COMMENT "Telephone area code applicable to the zipcode",
   TIMEZONE_NAME    VARCHAR(5)   COMMENT "Standard timezone designation. ** EST+1 refers to Atlantic Standard Time",
   IS_DST           CHAR(1)      COMMENT "Y - If zipcode participates in Daylight Saving Time. Else - N",
   HDP_INSERT_TS    TIMESTAMP
) STORED AS ORC;
