create table if not exists ext_NW_TOWER_USAGE.STATFLO_STORE_LIST_TEMP 
(
id   STRING  ,
banner   STRING  ,
channel   STRING  ,
location_dealer   STRING  ,
dealer_principal   STRING  ,
area_no   STRING  ,
store_manager   STRING  ,
dbm_am   STRING  ,
regional_director   STRING  ,
director_region   STRING  ,
mall_name   STRING  ,
enclosure   STRING  ,
location_tier   STRING  ,
location_type   STRING  ,
address   STRING  ,
unit_no   STRING  ,
city   STRING  ,
province   STRING  ,
subregion   STRING  ,
region   STRING  ,
cma   STRING  ,
postal_code   STRING  ,
telephone   STRING  ,
market_class_pop_cma   STRING  ,
sub_agent   STRING  ,
small_business_center   STRING  ,
store_design__fixture_type   STRING  ,
years_fixtures_installed   STRING  ,
rogers_head_lease   STRING  ,
cable_footprint   STRING  ,
chatr_sales   STRING  ,
dual_door   STRING  ,
traffic_counters   STRING  ,
sgi_lite   STRING  ,
shm   STRING  ,
fido_internet   STRING  ,
store_number_corp   STRING  ,
sgi_code   STRING  ,
sgi_name   STRING  ,
titan   STRING  ,
sc_store_code_current   STRING  ,
google_id   STRING  ,
opening_date   STRING  ,
m_and_a_date   STRING  ,
previous_ownership   STRING  ,
move_retro_location   STRING  ,
build_plan_action_date   STRING  ,
latitude   STRING  ,
longitude   STRING  ,
store_status   STRING  ,
date_closed   STRING  ,
lte   STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = "\,")
STORED AS TEXTFile
tblproperties ("skip.header.line.count"="1");




create table if not exists NW_TOWER_USAGE.STATFLO_STORE_LIST
(
id     varchar(5),
banner     varchar(9),
channel     varchar(12),
location_dealer     varchar(42),
dealer_principal     varchar(37),
area_no     varchar(2),
store_manager     varchar(27),
dbm_am     varchar(20),
regional_director     varchar(16),
director_region     varchar(6),
mall_name     varchar(46),
enclosure     varchar(14),
location_tier     varchar(16),
location_type     varchar(18),
address     varchar(51),
unit_no     varchar(20),
city     varchar(26),
province     varchar(2),
subregion     varchar(2),
region     varchar(7),
cma     varchar(40),
postal_code     varchar(6),
telephone     varchar(15),
market_class_pop_cma     varchar(1),
sub_agent     varchar(3),
small_business_center     varchar(3),
store_design__fixture_type     varchar(40),
years_fixtures_installed     date,
rogers_head_lease     varchar(3),
cable_footprint     varchar(3),
chatr_sales     double,
dual_door     varchar(3),
traffic_counters     varchar(32),
sgi_lite     varchar(3),
shm     varchar(3),
fido_internet     varchar(3),
store_number_corp     varchar(4),
sgi_code     varchar(2),
sgi_name     varchar(8),
titan     varchar(10),
sc_store_code_current     varchar(9),
google_id     varchar(10),
opening_date     date,
m_and_a_date     varchar(35),
previous_ownership     varchar(89),
move_retro_location     varchar(3),
build_plan_action_date     varchar(23),
latitude     varchar(10),
longitude     varchar(12),
store_status     varchar(12),
date_closed     date,
lte varchar(3)
)
STORED AS ORC;


create table if not exists NW_TOWER_USAGE.STATFLO_HOME_TOWER_TEMP
(
SUBSCRIBER_NO              VARCHAR(20) ,
ERP_SITE_LOCATION_CODE     VARCHAR(15) ,
ERP_SITE_NAME              VARCHAR(100),
CELLMAPNAME                VARCHAR(15) ,
X                          VARCHAR(20) ,
Y                          VARCHAR(20) ,
ADDRESS                    VARCHAR(254),
CITY                       VARCHAR(50) ,
PROVINCE                   VARCHAR(10) ,
HOME_SECTOR_CNT            INT
)
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'orc.row.index.stride'='50000',
'orc.stripe.size'='536870912');



create table if not exists NW_TOWER_USAGE.STATFLO_DISTANCE_INT
(
STORE_ID           varchar(5) ,
BANNER             varchar(9),
LOCATION_DEALER    varchar(42),
CMA                varchar(40),
ECID               BIGINT     ,
BAN                INT        ,
SUBSCRIBER_NO      VARCHAR(20),
DISTANCECALC       DECIMAL(5,2)
)
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'orc.row.index.stride'='50000',
'orc.stripe.size'='536870912');





create table if not exists NW_TOWER_USAGE.STATFLO_OUTPUT
(
STORE_ID              INT,
BANNER                VARCHAR(9),
LOCATION_DEALER       VARCHAR(40),
CMA                   VARCHAR(50),
ECID                  BIGINT ,
BAN                   INT,
SUBSCRIBER_NO         VARCHAR(20),
STORE_DISTANCE        DECIMAL(5,2),
STORE_DISTANCE_RNK    INT,
STORE_VISIT_DATE      TIMESTAMP ,
STORE_VISIT_DATE_RNK  INT
 )
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'orc.row.index.stride'='50000',
'orc.stripe.size'='536870912');

