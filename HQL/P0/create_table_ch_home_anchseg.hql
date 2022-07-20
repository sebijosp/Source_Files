create table data_pods.ch_pod_revenue_acct_dly3
(
enterprise_id varchar(50),
customer_id bigint,
customer_account bigint,
cr_key varchar(50),
customer_location_id bigint,
discount_tv double,
discount_rog_int double,
revenue_tv double,
revenue_rhp double,
revenue_int double,
tv_rev_gr decimal(38,2),
internet_rev_gr decimal(38,2),
rhp_rev_gr decimal(38,2),
calendar_month int,
calendar_year int)
partitioned by (process_date string);


create table data_pods.ch_pod_profile_acct_dly3
(
enterprise_id varchar(50),
customer_id bigint,
customer_account bigint,
cr_key varchar(50),
customer_location_id bigint,
age int,
postal_code varchar(6),
num_stb int,
tenure_month_tv bigint,
tenure_year_tv bigint,
product_source varchar(10),
flg_in_cablefootprint int,
flg_out_cablefootprint int,
flg_tv int,
flg_rhp int,
flg_shm int,
flg_internet int,
flg_selfserv int,
flg_analog_tv int,
calendar_month int,
calendar_year int)
partitioned by (process_date string);




create table data_pods.ch_pod_rhp_usage_acct_dly3
(
enterprise_id varchar(50),
customer_id bigint,
customer_account bigint,
rhp_mins_local bigint,
rhp_mins_ld bigint,
calendar_month int,
calendar_year int)
partitioned by (process_date string);





create table data_pods.ch_pod_tv_usage_acct_dly3
(
enterprise_id varchar(50),
customer_id bigint,
customer_account bigint,
flg_tv_add_tmn int,
tv_add_num_themepk int,
avg_tv_dur_hrs_kids_fmly decimal(38,2),
avg_tv_dur_hrs_life_real decimal(38,2),
avg_tv_dur_hrs_movies decimal(38,2),
avg_tv_dur_hrs_multicul decimal(38,2),
avg_tv_dur_hrs_shop decimal(38,2),
avg_tv_dur_hrs_spirit decimal(38,2),
avg_tv_dur_hrs_sports decimal(38,2),
avg_tv_num_channel decimal(38,2),
avg_tv_num_genre decimal(38,2),
avg_tv_dur_hrs_200 decimal(38,2),
avg_tv_dur_hrs_600 decimal(38,2),
avg_tv_dur_hrs_1000 decimal(38,2),
avg_tv_dur_hrs_1630 decimal(38,2),
avg_tv_dur_hrs_1930 decimal(38,2),
avg_tv_dur_hrs_2300 decimal(38,2),
avg_tv_dur_hrs_cannetwork decimal(38,2),
avg_tv_dur_hrs_unknown decimal(38,2),
avg_tv_dur_hrs_weekday decimal(38,2),
avg_tv_dur_hrs_weekend decimal(38,2),
avg_tv_num_day_view decimal(38,2),
avg_tv_ttl_dur_hrs_mly decimal(38,2),
avg_rod_cnt_children decimal(38,2),
avg_rod_cnt_comedy decimal(38,2),
avg_rod_cnt_drama decimal(38,2),
avg_rod_cnt_family decimal(38,2),
calendar_month int,
calendar_year int)
partitioned by(process_date string);



create table data_pods.ch_pod_int_usage_acct_dly3
(
enterprise_id varchar(50),
customer_id bigint,
customer_account bigint,
internet_mly_usage_actual decimal(38,2),
internet_mly_usage_entitled bigint,
flg_int_10to19mbps int,
calendar_month int,
calendar_year int)
partitioned by (process_date string);
