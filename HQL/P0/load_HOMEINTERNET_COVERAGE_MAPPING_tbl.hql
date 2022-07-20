!echo ;
!echo Hive : Dropping External Table IPTV.HOMEINTERNET_COVERAGE_MAPPING_TEMP;
DROP TABLE IF EXISTS IPTV.HOMEINTERNET_COVERAGE_MAPPING_TEMP;

!echo ;
!echo INFO Hive : Creating temporary external table IPTV.HOMEINTERNET_COVERAGE_MAPPING_TEMP at '/userapps/iptv/homeinternet_coverage_mapping';

create external table IPTV.HOMEINTERNET_COVERAGE_MAPPING_TEMP(
    SOURCE  String,
    MARKER  String,
    TYPE  String,
    CATEGORY  String,
    FUNCTIONALITY  String )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/userapps/iptv/homeinternet_coverage_mapping'
tblproperties ("skip.header.line.count"="1");

!echo ;
!echo Hive : Temp Table created;

create table if not exists IPTV.HOMEINTERNET_COVERAGE_MAPPING(
SOURCE  String,
MARKER  String,
TYPE  String,
CATEGORY  String,
FUNCTIONALITY  String,
HDP_INSERT_TS  Timestamp,
HDP_UPDATE_TS  Timestamp
)stored as ORC;

!echo Hive : Loading data into table IPTV.HOMEINTERNET_COVERAGE_MAPPING;


insert overwrite table IPTV.HOMEINTERNET_COVERAGE_MAPPING
select
SOURCE ,
MARKER ,
TYPE ,
CATEGORY ,
FUNCTIONALITY ,
from_unixtime(unix_timestamp()),
from_unixtime(unix_timestamp())
FROM IPTV.HOMEINTERNET_COVERAGE_MAPPING_TEMP;

!echo ;
!echo Hive : Data loaded into IPTV.HOMEINTERNET_COVERAGE_MAPPING;

