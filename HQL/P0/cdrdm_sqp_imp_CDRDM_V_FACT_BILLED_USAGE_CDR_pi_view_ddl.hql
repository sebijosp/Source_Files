use cdrdm;
DROP VIEW IF EXISTS v_fact_billed_usage_cdr;
CREATE VIEW v_fact_billed_usage_cdr AS 
SELECT
ban, 
bill_seq_no, 
subscriber_no, 
channel_seizure_dt, 
message_switch_id, 
REGEXP_REPLACE(call_to_tn, '[|]','=') as call_to_tn,
sys_creation_date, 
sys_update_date, 
operator_id, 
application_id, 
dl_service_code, 
dl_update_stamp, 
soc, 
soc_date, 
currency_code, 
feature_code, 
rate_eff_date, 
rated_ftr_lvl, 
rating_level_code, 
price_plan_code, 
service_ftr_seq_no, 
call_action_code, 
rate_action_code, 
feature_selection_dt, 
product_type, 
bl_prsnt_ftr_cd, 
rating_fu_type, 
unit_esn,
COALESCE(ring_time,0) AS ring_time,
COALESCE(recorded_units,0) AS recorded_units,
recorded_units_uom, 
COALESCE(rounded_units,0) AS rounded_units,
COALESCE (units_to_rate, 0) AS units_to_rate, 
COALESCE (num_of_iu, 0) AS num_of_iu, 
COALESCE (charge_amt, 0) AS charge_amt, 
COALESCE(charge_amt_foreign,0) AS charge_amt_foreign, 
COALESCE(charge_amt_incl_iu,0) AS charge_amt_incl_iu, 
CASE WHEN call_start_period IS NULL OR TRIM(call_start_period) = '' OR TRIM(call_start_period) = '00' THEN '99' ELSE TRIM(call_start_period) END AS call_start_period,
CASE WHEN call_end_period IS NULL OR TRIM(call_end_period) = '' OR TRIM(call_end_period) = '00' THEN '99' ELSE TRIM(call_end_period) END AS call_end_period,
call_term_code, 
call_to_city_desc, 
call_from_state_code, 
call_to_state_code, 
orig_v_coordinate, 
orig_h_coordinate, 
term_v_coordinate, 
term_h_coordinate, 
bill_presentation_no, 
serve_sid, 
serve_cell, 
serve_province, 
serve_place, 
home_sid, 
home_province, 
mps_file_number, 
toll_rate_period, 
toll_type, 
mps_ld_feature_code, 
COALESCE (toll_charge_amt, 0) AS toll_charge_amt, 
COALESCE (toll_charge_amt_foreign, 0) AS toll_charge_amt_foreign, 
COALESCE(toll_dur_sec,0) AS toll_dur_sec, 
COALESCE(toll_dur_round_min,0) AS toll_dur_round_min, 
COALESCE (special_num_type, 'X') AS special_num_type, 
market_code, 
sub_market_code, 
COALESCE (orig_air_occ_charge, 0) AS orig_air_occ_charge, 
COALESCE (orig_air_occ_tax, 0) AS orig_air_occ_tax, 
incollect_rerate_ind, 
message_type, 
ac_free_code, 
COALESCE (ac_amt, 0) AS ac_amt, 
COALESCE(ac_amt_foreign,0) AS ac_amt_foreign, 
ac_soc, 
ac_soc_date, 
ac_currency_code, 
COALESCE(rm_tax_amt_toll,0) AS rm_tax_amt_toll, 
COALESCE(rm_tax_amt_air,0) AS rm_tax_amt_air, 
COALESCE(rm_tax_amt_ac,0) AS rm_tax_amt_ac, 
tax_province, 
tax_waive_ind, 
toll_tax_waive_ind, 
charge_free_code, 
toll_free_code, 
call_direction_code, 
call_type_feature, 
call_cross_dt_ind, 
call_cross_prd_ind, 
call_cross_stp_ind, 
call_cross_im_ind,
cycle_run_year, 
cycle_run_month, 
cycle_code,
cancel_record_ind, 
call_adjustment_ind, 
bl_rerate_code, 
bl_rerate_date, 
activity_resolve_cd, 
mps_feature_code_1, 
mps_feature_code_2, 
mps_feature_code_3, 
mps_feature_code_4, 
mps_feature_code_5, 
fuyt_ind, 
roam_soc, 
rate_group, 
extended_hm_area_ind, 
price_plan_eff_date, 
soc_seq_no, 
roam_soc_eff_dt, 
roam_soc_rt_eff_dt, 
toll_soc, 
toll_soc_seq_no, 
toll_currency_code, 
toll_feature_code, 
toll_rate_action_code, 
toll_serv_ftr_seq_no, 
toll_soc_date, 
toll_rate_eff_date, 
toll_rating_level, 
toll_rated_ftr_lvl, 
COALESCE(toll_num_mins_to_rate,0) AS toll_num_mins_to_rate, 
COALESCE(toll_num_of_im,0) AS toll_num_of_im, 
toll_start_prd, 
COALESCE(toll_chrg_amt_incl_im,0) AS toll_chrg_amt_incl_im, 
free_units_ind, 
COALESCE(num_of_free_units,0) AS num_of_free_units, 
format_source, 
record_id, 
in_route, 
out_route, 
COALESCE(orig_toll_charge,0) AS orig_toll_charge, 
COALESCE(orig_toll_tax,0) AS orig_toll_tax, 
rjct_sending_sid, 
rjct_seq, 
rjct_rrc, 
outcol_source, 
extended_grace_ind, 
units_to_rate_uom, 
toll_uom_code, 
exact_pricing_ind, 
exact_pric_soc, 
exact_pric_feature, 
exact_pric_eff_date, 
exact_pric_action_code, 
ban_level_1, 
ban_level_2, 
ban_level_3, 
fict_sid, 
ems_counter, 
ac_feature_code, 
ac_rate_eff_date, 
ac_rate_action_code, 
COALESCE(rate,0) as rate, 
ac_serv_ftr_seq_no, 
ac_soc_seq_no, 
imsi, 
guide_by, 
call_to_imsi, 
call_to_type, 
serve_plmn, 
home_plmn, 
product_sub_type, 
utc_offset, 
cdr_source_id, 
special_fm_eligibility, 
mms_agent, 
number_of_recipients, 
mms_type, 
mms_sub_type, 
bearer_channel, 
mms_class, 
mms_delivery_type, 
toll_rating_fu_type, 
ps_air, 
ps_toll, 
sur_rur_id, 
sur_process_date, 
sur_report_id, 
record_type_ind, 
apn, 
serve_country_cd, 
in_ind, 
COALESCE(ld_rate,0) AS ld_rate, 
mps_ld_feature_code_1, 
mps_ld_feature_code_2, 
jv_site, 
us_stream_type, 
dom_fallback1, 
dom_fallback2, 
surchrg_soc, 
surchrg_ftr, 
units_not_rated,
chrg_amt_not_rated,
spcl_rec_ind,
trim(substring(trim(dom_fallback1),1,6)) as dom_mps_id_feature_code, 
trim(substring(trim(dom_fallback1),7,6)) as dom_mps_feature_code_1,
trim(substring(trim(dom_fallback1),13,6)) as dom_mps_feature_code_2,
trim(substring(trim(dom_fallback1),19,6)) as dom_mps_feature_code_3,
trim(substring(trim(dom_fallback1),25,6)) as dom_mps_feature_code_4,
trim(substring(trim(dom_fallback2),1,2)) as dom_toll_type,
trim(substring(trim(dom_fallback2),3,2)) as dom_charge_free_code,
trim(substring(trim(dom_fallback2),5,2)) as dom_toll_free_code,
trim(substring(trim(dom_fallback2),7,2)) as dom_ac_free_code,
concat_ws('/',mps_feature_code_1,mps_feature_code_2,mps_feature_code_3,mps_feature_code_4,mps_feature_code_5) AS mps_feature_code,
((cycle_run_year * 12)+(cycle_run_month + 100)) as time_key
FROM elau_usage.usage_billed
WHERE cancel_record_ind='N';
