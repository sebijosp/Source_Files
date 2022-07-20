DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_CUST_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_CUST_TEMP
(
subscriber_no           varchar(20),
ban                     int,
brand                   varchar(10)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_HOME_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_HOME_TEMP 
(
subscriber_no          varchar(20),
erp_site_location_code varchar(15),
erp_site_name          varchar(100),
x                      varchar(20),
y                      varchar(20),
address                varchar(254),
city                   varchar(50),
province               varchar(10),
days_usage_count       bigint,
tower_usage_count      bigint,
most_used_tower        int 
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_DISTANCE_TEMP_OR;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_DISTANCE_TEMP_OR 
(
subscriber_no           varchar(20),
ban                     int,
brand                   varchar(10),
ctn_lat                 varchar(20),
ctn_lon                 varchar(20)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_DISTANCE_OR;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_DISTANCE_OR 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  varchar(50),
location_name           varchar(140),
cma                     varchar(255),
location_type           varchar(510),
sq_ft                   decimal(20,0),
subscriber_no           varchar(20),
ban                     int,
distancecalc            decimal(5,2)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_OR_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_OR_TEMP 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  varchar(50),
location_name           varchar(140),
cma                     varchar(255),
location_type           varchar(510),
sq_ft                   decimal(20,0),
subscriber_no           varchar(20),
ban                     int,
distancecalc            decimal(5,2),
store_distance_rnk      int
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_OUTPUT_PRE;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_OUTPUT_PRE
(
store_id                int,
banner                  varchar(9),
location_name           varchar(140),
cma                     varchar(50),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          decimal(5,2),
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
chain_code              varchar(20),
location_id             varchar(100),
lead_logic              string,
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_OUTPUT;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_OUTPUT
(
store_id                int,
banner                  varchar(9),
location_name           varchar(140),
cma                     varchar(50),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          decimal(5,2),
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
chain_code              varchar(20),
location_id             varchar(100),
lead_logic              string,
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_DISTANCE_OR;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_DISTANCE_OR 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  varchar(50),
location_name           varchar(140),
cma                     varchar(255),
location_type           varchar(510),
sq_ft                   decimal(20,0),
subscriber_no           varchar(20),
ban                     int,
distancecalc            decimal(5,2)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_OR_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_OR_TEMP 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  varchar(50),
location_name           varchar(140),
cma                     varchar(255),
location_type           varchar(510),
sq_ft                   decimal(20,0),
subscriber_no           varchar(20),
ban                     int,
distancecalc            decimal(5,2),
store_distance_rnk      int
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_OUTPUT_PRE;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_OUTPUT_PRE
(
store_id                int,
banner                  varchar(9),
location_name           varchar(140),
cma                     varchar(50),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          decimal(5,2),
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
chain_code              varchar(20),
location_id             varchar(100),
lead_logic              string,
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_OUTPUT;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CHATR_P2P_OUTPUT
(
store_id                int,
banner                  varchar(9),
location_name           varchar(140),
cma                     varchar(50),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          decimal(5,2),
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
chain_code              varchar(20),
location_id             varchar(100),
lead_logic              string,
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

ALTER TABLE NW_TOWER_USAGE.STATFLO_OUTPUT CHANGE CABLE_FOOTPRINT STORE_CABLE_FOOTPRINT VARCHAR(10);
ALTER TABLE NW_TOWER_USAGE.STATFLO_OUTPUT ADD COLUMNS (CUSTOMER_BRAND  VARCHAR(9));
