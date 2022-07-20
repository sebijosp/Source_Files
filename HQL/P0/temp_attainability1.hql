Insert into hem.modem_attainability_weekly_bd1 PARTITION(year,week)
select 
  cm_mac_addr,cmts_host_name,cmts_md_if_index,CMTS_MD_IF_NAME,MODEM_MAKE,MODEM_MODEL,
  PRODUCT_CODE,TIER,technology,ds_tuners_count,us_tuners_count,download_speed_kbps,
  upload_speed_kbps,download_speed_mbps,upload_speed_mbps,cbu_code,full_acct_num,
  account,dtv_model_num,dtv_serial_num,system,primary_hub,secondary_hub,smt,
  municipality,equipment_status_code,billing_src_mac_id,postal_zip_code,NODE,
  RTN_SEG,Clamshell,Firmware,BB_TIER,HH_TYPE,CONTRACT_TYPE,timezone,is_dst,
  SUM(count_nw_actual_ds_impact_full) as count_actual_ds_impact_full,
  SUM(count_nw_actual_dsus_impact_full) as count_actual_dsus_impact_full,
  SUM(count_nw_actual_us_impact_full) as count_actual_us_impact_full,
  SUM(total_counters_nw_full) as total_counters_full,
  SUM(count_nw_actual_ds_impact_all) as count_actual_ds_impact_all,
  sum(count_nw_actual_dsus_impact_all) as count_actual_dsus_impact_all,
  sum(count_nw_actual_us_impact_all) as count_actual_us_impact_all,
  SUM(total_counters_nw_all) as total_counters_all,
  sum(count_nw_actual_ds_impact_prime) as count_actual_ds_impact_prime,
  sum(count_nw_actual_dsus_impact_prime) as count_actual_dsus_impact_prime,
  sum(count_nw_actual_us_impact_prime) as count_actual_us_impact_prime,
  SUM(total_counters_nw_prime) as total_counters_prime,
  SUM(ds_speed_attainable_full) as ds_speed_attainable_full,
  SUM(ds_speed_attainable_all) as ds_speed_attainable_all,
  SUM(ds_speed_attainable_prime) as ds_speed_attainable_prime,
  SUM(us_speed_attainable_full) as us_speed_attainable_full,
  SUM(us_speed_attainable_all) as us_speed_attainable_all,
  SUM(us_speed_attainable_prime) as us_speed_attainable_prime,
  MAX(potential_impact_ds) as potential_impact_ds,
  MAX(actual_impact_ds) as actual_impact_ds,
  MAX(potential_impact_dsus) as potential_impact_dsus,
  MAX(actual_impact_dsus) as actual_impact_dsus,
  MAX(potential_impact_us) as potential_impact_us,
  MAX(actual_impact_us) as actual_impact_us,
  MAX(potential_impact) as potential_impact,
  MAX(actual_impact) as actual_impact,
  SUM(actual_impact_counter) as actual_impact_counter,
  SUM(TTL) as TTL,
  AVG(ATTAINABILITY_PCT) as ATTAINABILITY_PCT,
  AVG(UP_MBYTES) as UP_MBYTES,
  AVG(DOWN_MBYTES) as DOWN_MBYTES,
  AVG(TOTAL_USG_MBYTES) as TOTAL_USG_MBYTES,
  SUM(ACTUAL_IMPACT_COUNTER_FULL) as ACTUAL_IMPACT_COUNTER_FULL,
  SUM(ACTUAL_IMPACT_COUNTER_PRIME) as ACTUAL_IMPACT_COUNTER_PRIME,
  date_sub('2020-12-29', 1) as event_date
  , current_timestamp as hdp_insert_ts
  , current_timestamp as hdp_update_ts
  , 2020 as year
  , 53 as week
from 
  hem.modem_attainability_daily
where 
  event_date >='2020-12-28' and event_date < '2021-01-04'
group by 
  cm_mac_addr,cmts_host_name,cmts_md_if_index,CMTS_MD_IF_NAME,MODEM_MAKE,MODEM_MODEL,
  PRODUCT_CODE,TIER,technology,ds_tuners_count,us_tuners_count,download_speed_kbps,
  upload_speed_kbps,download_speed_mbps,upload_speed_mbps,cbu_code,full_acct_num,
  account,dtv_model_num,dtv_serial_num,system,primary_hub,secondary_hub,smt,
  municipality,equipment_status_code,billing_src_mac_id,postal_zip_code,NODE,
  RTN_SEG,Clamshell,Firmware,BB_TIER,HH_TYPE,CONTRACT_TYPE,timezone,is_dst;
