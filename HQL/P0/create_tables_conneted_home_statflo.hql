DROP TABLE NW_TOWER_USAGE.STATFLO_CH_CUST_TEMP;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CH_CUST_TEMP
(
ecid                varchar(30),
lst_franchise_tp    varchar(3),
ctn_lat             decimal(12,6),
ctn_lon             decimal(10,6)
);

DROP TABLE NW_TOWER_USAGE.STATFLO_CONNECTED_HOME_OUTPUT;
CREATE TABLE NW_TOWER_USAGE.STATFLO_CONNECTED_HOME_OUTPUT
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



