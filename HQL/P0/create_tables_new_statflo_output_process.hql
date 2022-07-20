DROP TABLE IF EXISTS NW_TOWER_USAGE.STATFLO_DISTANCE_INT;

create table if not exists NW_TOWER_USAGE.STATFLO_DISTANCE_INT
(
STORE_ID           varchar(5) ,
BANNER             varchar(9),
LOCATION_DEALER    varchar(42),
CMA                varchar(40),
ECID               BIGINT     ,
BAN                INT        ,
SUBSCRIBER_NO      VARCHAR(20),
DISTANCECALC       DECIMAL(5,2),
CHAIN_CODE         VARCHAR(20),
LOCATION_ID        VARCHAR(100) 
)
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'orc.row.index.stride'='50000',
'orc.stripe.size'='536870912');

DROP TABLE IF EXISTS NW_TOWER_USAGE.STATFLO_OUTPUT;

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
STORE_VISIT_DATE_RNK  INT       ,
CHAIN_CODE            VARCHAR(20),
LOCATION_ID           VARCHAR(100)
 )
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'orc.row.index.stride'='50000',
'orc.stripe.size'='536870912');
