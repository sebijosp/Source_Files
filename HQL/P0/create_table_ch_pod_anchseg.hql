DROP TABLE IF EXISTS data_pods.ch_pod_revenue_acct_dly;
CREATE TABLE data_pods.ch_pod_revenue_acct_dly (
  enterprise_id varchar(50),
  customer_id bigint,
  customer_account bigint,
  cr_key varchar(50),
  customer_location_id bigint,
  discount_tv DOUBLE,
  discount_rhp DOUBLE,
  discount_rog_int DOUBLE,
  discount_fido_int DOUBLE,
  discount_shm DOUBLE,
  revenue_tv DOUBLE,
  revenue_rhp DOUBLE,
  revenue_int DOUBLE,
  revenue_fido_int DOUBLE,
  rev_net_shm DOUBLE,
  flg_rogers INT,
  flg_fido INT,
  brand STRING,
  tv_rev_gr decimal(38, 2),
  internet_rev_gr decimal(38, 2),
  rhp_rev_gr decimal(38, 2)) 
PARTITIONED BY (
  calendar_month int,
  calendar_year int,
  process_date string ) ;
 
DROP TABLE IF EXISTS data_pods.ch_pod_profile_acct_dly; 
CREATE TABLE data_pods.ch_pod_profile_acct_dly (
  enterprise_id varchar(50),
  customer_id bigint,
  customer_account bigint,
  cr_key varchar(50),
  customer_location_id bigint,
  age INT,
  postal_code VARCHAR(6),
  num_stb INT,
  tenure_month_tv bigint,
  tenure_month_rint bigint,
  tenure_month_rhp bigint,
  tenure_month_fint bigint,
  tenure_month_shm bigint,
  tenure_year_tv bigint,
  tenure_year_rint bigint,
  tenure_year_rhp bigint,
  tenure_year_fint bigint,
  tenure_year_shm bigint,
  product_source STRING,
  flg_in_cablefootprint INT,
  flg_out_cablefootprint INT,
  flg_tv INT,
  flg_rhp INT,
  flg_shm INT,
  flg_internet INT,
  flg_rogers INT,
  flg_fido INT,
  brand STRING,
  flg_selfserv INT,
  flg_analog_tv INT ) 
PARTITIONED BY (
  calendar_month int,
  calendar_year int,
  process_date string ) ;

DROP TABLE IF EXISTS data_pods.ch_pod_rhp_usage_acct_dly;
CREATE TABLE data_pods.ch_pod_rhp_usage_acct_dly (
  enterprise_id varchar(50),
  customer_id bigint,
  customer_account bigint,
  rhp_mins_local decimal(38, 2),
  rhp_mins_ld decimal(38, 2)) 
PARTITIONED BY (
  calendar_month int,
  calendar_year int,
  process_date string )  ;

DROP TABLE IF EXISTS data_pods.ch_pod_tv_usage_acct_dly;  
CREATE TABLE data_pods.ch_pod_tv_usage_acct_dly (
  enterprise_id varchar(50),
  customer_id bigint,
  customer_account bigint,
  flg_tv_add_tmn INT,
  tv_add_num_themepk BIGINT,
  avg_tv_dur_hrs_kids_fmly decimal(38, 2),
  avg_tv_dur_hrs_life_real decimal(38, 2),
  avg_tv_dur_hrs_movies decimal(38, 2),
  avg_tv_dur_hrs_multicul decimal(38, 2),
  avg_tv_dur_hrs_shop decimal(38, 2),
  avg_tv_dur_hrs_spirit decimal(38, 2),
  avg_tv_dur_hrs_sports decimal(38, 2),
  avg_tv_num_channel decimal(38, 2),
  avg_tv_num_genre decimal(38, 2),
  avg_tv_dur_hrs_200 decimal(38, 2),
  avg_tv_dur_hrs_600 decimal(38, 2),
  avg_tv_dur_hrs_1000 decimal(38, 2),
  avg_tv_dur_hrs_1630 decimal(38, 2),
  avg_tv_dur_hrs_1930 decimal(38, 2),
  avg_tv_dur_hrs_2300 decimal(38, 2),
  avg_tv_dur_hrs_cannetwork decimal(38, 2),
  avg_tv_dur_hrs_unknown decimal(38, 2),
  avg_tv_dur_hrs_weekday decimal(38, 2),
  avg_tv_dur_hrs_weekend decimal(38, 2),
  avg_tv_num_day_view decimal(38, 2),
  avg_tv_ttl_dur_hrs_mly decimal(38, 2),
  avg_rod_cnt_children decimal(38, 2),
  avg_rod_cnt_comedy decimal(38, 2),
  avg_rod_cnt_drama decimal(38, 2),
  avg_rod_cnt_family decimal(38, 2)) 
PARTITIONED BY (
  calendar_month int,
  calendar_year int,
  process_date string )  ;

DROP TABLE IF EXISTS data_pods.ch_pod_int_usage_acct_dly;
CREATE TABLE data_pods.ch_pod_int_usage_acct_dly (
  enterprise_id varchar(50),
  customer_id bigint,
  customer_account bigint,
  flg_rogers INT,
  flg_fido INT,
  brand STRING,
  internet_mly_usage_actual decimal(38, 2),
  internet_mly_usage_entitled bigint,
  flg_int_10to19mbps INT) 
PARTITIONED BY (
  calendar_month int,
  calendar_year int,
  process_date string )  ;
