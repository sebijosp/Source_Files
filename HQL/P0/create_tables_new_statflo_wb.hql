DROP TABLE NW_TOWER_USAGE.STATFLO_WB_CUST_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_CUST_TEMP
(
ecid              bigint,
ban               int,
subscriber_no     varchar(20),
franchise_tp      varchar(3),
disconnect_date   timestamp,
deact_reason_code varchar(4),
deact_reason_desc varchar(40),
port_flag         varchar(1),
port_out_flag     varchar(1),
new_carrier       varchar(40)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_HOME_TOWER_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_HOME_TOWER_TEMP 
(
subscriber_no           varchar(20),
erp_site_location_code  varchar(15),
erp_site_name           varchar(100),
cellmapname             varchar(15),
x                       varchar(20),
y                       varchar(20),
address                 varchar(254),
city                    varchar(50),
province                varchar(10),
home_sector_cnt         int
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_TEMP 
(
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
franchise_tp            varchar(3),
ctn_lat                 varchar(20),
ctn_lon                 varchar(20)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_INT;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_INT 
(
store_id                varchar(5),
banner                  varchar(9),
location_name           varchar(140),
cma                     varchar(40),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
distancecalc            decimal(5,2),
chain_code              varchar(20),
location_id             varchar(100),
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_STORE_VISIT;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_STORE_VISIT 
(
ban                     int,
brsc_id                 decimal(18,0),
trans_date              timestamp,
row_num                 int
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_AND;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_AND 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  varchar(50),
location_name           varchar(140),
cma                     varchar(255),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          decimal(5,2),
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
lead_logic              varchar(255),
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_TEMP_OR;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_TEMP_OR 
(
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
franchise_tp            varchar(3),
ctn_lat                 varchar(20),
ctn_lon                 varchar(20)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_OR;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_DISTANCE_OR 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  varchar(50),
location_name           varchar(140),
cma                     varchar(255),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
distancecalc            decimal(5,2),
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_OR_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_OR_TEMP 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  string,
location_name           varchar(140),
cma                     varchar(255),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
distancecalc            double,
store_distance_rnk      int,
location_type           varchar(510),
sq_ft                   decimal(20,0)
);


DROP TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_OR;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_OR 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  varchar(50),
location_name           varchar(140),
cma                     varchar(255),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          decimal(5,2),
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
lead_logic              varchar(255),
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_PRE;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_PRE 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  string,
location_name           varchar(140),
cma                     varchar(255),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          double,
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
lead_logic              string,
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_SECONDARY;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT_SECONDARY 
(
store_id                varchar(510),
chain_code              varchar(20),
location_id             varchar(100),
banner                  string,
location_name           varchar(140),
cma                     varchar(255),
ecid                    bigint,
ban                     int,
subscriber_no           varchar(20),
store_distance          double,
store_distance_rnk      int,
store_visit_date        timestamp,
store_visit_date_rnk    int,
lead_logic              string,
location_type           varchar(510),
sq_ft                   decimal(20,0)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT;
CREATE TABLE NW_TOWER_USAGE.STATFLO_WB_OUTPUT 
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

DROP TABLE NW_TOWER_USAGE.STATFLO_OUTPUT;
CREATE TABLE NW_TOWER_USAGE.STATFLO_OUTPUT 
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
statflo_customer_type   varchar(1000),
location_type           varchar(510),
sq_ft                   decimal(20,0)
);
