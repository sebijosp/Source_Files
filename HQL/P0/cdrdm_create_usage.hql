--[Version History]
--0.1 - danchang - 4/06/2016 - latest version
--
USE ods_usage;

DROP TABLE z_usage_temp;

CREATE TABLE z_usage_temp(
usage_id decimal(38,0),
ban bigint,
bill_seq_no int,
subscriber_no varchar(20),
channel_seizure_dt string,
message_switch_id char(6),
call_to_tn char(15),
sys_creation_date string,
sys_update_date string,
operator_id bigint,
application_id char(6),
dl_service_code char(5),
dl_update_stamp int,
soc char(9),
soc_date string,
currency_code char(3),
feature_code char(6),
rate_eff_date string,
rated_ftr_lvl char(1),
rating_level_code char(1),
price_plan_code char(9),
service_ftr_seq_no bigint,
call_action_code char(1),
rate_action_code char(1),
feature_selection_dt string,
product_type char(1),
bl_prsnt_ftr_cd char(4),
rating_fu_type char(2),
unit_esn char(20),
ring_time bigint,
recorded_units bigint,
recorded_units_uom char(2),
rounded_units decimal(11,2),
units_to_rate decimal(11,2),
num_of_iu decimal(11,2),
charge_amt decimal(9,2),
charge_amt_foreign decimal(9,2),
charge_amt_incl_iu decimal(9,2),
call_start_period char(2),
call_end_period char(2),
call_term_code char(1),
call_to_city_desc varchar(28),
call_from_state_code char(2),
call_to_state_code char(2),
orig_v_coordinate int,
orig_h_coordinate int,
term_v_coordinate int,
term_h_coordinate int,
bill_presentation_no char(15),
serve_sid char(5),
serve_cell char(7),
serve_province char(2),
serve_place varchar(28),
home_sid char(5),
home_province char(2),
mps_file_number bigint,
toll_rate_period char(2),
toll_type char(1),
mps_ld_feature_code char(6),
toll_charge_amt decimal(9,2),
toll_charge_amt_foreign decimal(9,2),
toll_dur_sec bigint,
toll_dur_round_min decimal(7,2),
special_num_type char(1),
market_code char(3),
sub_market_code char(3),
orig_air_occ_charge decimal(9,2),
orig_air_occ_tax decimal(9,2),
incollect_rerate_ind char(1),
message_type char(1),
ac_free_code char(2),
ac_amt decimal(9,2),
ac_amt_foreign decimal(9,2),
ac_soc char(9),
ac_soc_date string,
ac_currency_code char(3),
rm_tax_amt_toll decimal(9,2),
rm_tax_amt_air decimal(9,2),
rm_tax_amt_ac decimal(9,2),
tax_province char(2),
tax_wave_ind char(1),
toll_tax_waive_ind char(1),
charge_free_code char(2),
toll_free_code char(2),
call_direction_code char(1),
call_type_feature char(6),
call_cross_dt_ind char(1),
call_cross_prd_ind char(1),
call_cross_stp_ind char(1),
call_cross_im_ind char(1),
--Removed because they are part of the PARTITION statement
--cycle_code int,
--cycle_run_year int,
--cycle_run_month int,
cancel_record_ind char(1),
call_adjustment_ind char(1),
bl_rerate_code char(1),
bl_rerate_date string,
activity_resolve_cd char(2),
mps_feature_code_1 char(6),
mps_feature_code_2 char(6),
mps_feature_code_3 char(6),
mps_feature_code_4 char(6),
mps_feature_code_5 char(6),
fuyt_ind char(1),
roam_soc char(9),
rate_group char(1),
extended_hm_area_ind char(1),
price_plan_eff_date string,
soc_seq_no bigint,
roam_soc_eff_dt string,
roam_soc_rt_eff_dt string,
toll_soc char(9),
toll_soc_seq_no bigint,
toll_currency_code char(3),
toll_feature_code char(6),
toll_rate_action_code char(1),
toll_serv_ftr_seq_no bigint,
toll_soc_date string,
toll_rate_eff_date string,
toll_rating_level char(1),
toll_rated_ftr_lvl char(1),
toll_num_mins_to_rate decimal(7,2),
toll_num_of_im decimal(7,2),
toll_start_prd char(2),
toll_chrg_amt_incl_im decimal(9,2),
free_units_ind char(1),
num_of_free_units decimal(9,2),
format_source char(8),
record_id char(5),
in_route char(8),
out_route char(8),
orig_toll_charge decimal(9,2),
orig_toll_tax decimal(9,2),
rjct_sending_sid char(5),
rjct_seq int,
rjct_rrc int,
outcol_source char(1),
extended_grace_ind char(1),
units_to_rate_uom char(2),
toll_uom_code char(2),
exact_pricing_ind char(1),
exact_pric_soc char(9),
exact_pric_feature char(6),
exact_pric_eff_date string,
exact_pric_action_code char(1),
ban_level_1 bigint,
ban_level_2 bigint,
ban_level_3 bigint,
fict_sid char(5),
ems_counter bigint,
ac_feature_code char(6),
ac_rate_eff_date string,
ac_rate_action_code char(1),
rate decimal(12,5),
ac_serv_ftr_seq_no bigint,
ac_soc_seq_no bigint,
imsi char(15),
guide_by char(1),
call_to_imsi char(15),
call_to_type char(1),
serve_plmn varchar(6),
home_plmn varchar(6),
product_sub_type char(2),
utc_offset int,
cdr_source_id int,
special_fm_eligibility char(1),
mms_agent char(64),
number_of_recipients bigint,
mms_type varchar(8),
mms_sub_type varchar(8),
bearer_channel varchar(8),
mms_class char(1),
mms_delivery_type char(1),
toll_rating_fu_type char(2),
ps_air char(4),
ps_toll char(4),
sur_rur_id char(1),
sur_process_date string,
sur_report_id char(1),
record_type_ind char(1),
apn char(64),
serve_country_cd int,
in_ind char(1),
ld_rate decimal(12,5),
mps_ld_feature_code_1 char(6),
mps_ld_feature_code_2 char(6),
jv_site char(3),
us_stream_type char(1),
dom_fallback1 varchar(30),
dom_fallback2 varchar(8),
surchrg_soc char(9),
surchrg_ftr char(6),
etl_created_workflow_run_id decimal(38,0),
etl_created_ts string,
gg_src_dbname varchar(20),
gg_op_type varchar(1),
gg_commit_ts string,
gg_replicat_name varchar(20),
gg_load_to_tdat_ts string,
gg_load_sequence bigint,
gg_log_rba varchar(20),
gg_source_ts string,
gg_bigdata_scn varchar(20),
gg_bigdata_op_type varchar(1),
gg_bigdata_log_position varchar(20),
gg_datalake_load_ts string,
-- NEW COLUMNS
tr_account_sub_type CHAR(1),
tr_company_code VARCHAR(15),
tr_franchise_cd VARCHAR(5),
tr_ba_account_type CHAR(1),
tr_sub_ucc_ind VARCHAR(50),
sqoop_ext_date STRING,
insert_timestamp STRING
)
PARTITIONED BY (cycle_run_year INT, cycle_run_month INT, cycle_code INT)
STORED AS ORC TBLPROPERTIES ("ORC.COMPRESS"="SNAPPY","orc.row.index.stride"="50000","orc.stripe.size"="536870912");

ALTER TABLE z_usage_temp SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=4 and cycle_code=14;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=4 and cycle_code=15;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=10;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=11;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=12;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=13;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=16;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=17;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=18;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=19;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=2;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=20;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=21;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=22;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=23;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=24;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=25;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=26;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=27;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=28;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=29;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=3;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=30;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=35;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=38;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=4;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=5;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=6;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=7;

INSERT INTO TABLE z_usage_temp PARTITION (cycle_run_year, cycle_run_month, cycle_code) 
SELECT * FROM usage where cycle_run_year=2016 and cycle_run_month=5 and cycle_code=8;

DROP TABLE usage;
ALTER TABLE z_usage_temp RENAME TO usage;