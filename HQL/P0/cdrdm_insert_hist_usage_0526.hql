--[Version History]
--0.1 - danchang - 4/06/2016 - Latest version from Alfaz, using dummy tables and ext_ela_v21 test tables
--e.g. ext_ods_usage.test_usage -> ext_ods_usage.usage_DUMMY, ELA_V21 -> EXT_ELA_V21
--0.2 - danchang - 4/25/2016 - Updated references for PROD deployment
--

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;

----------------------------------- Preparing TEMP table for ALS_IND ----------------------------------------------------

DROP TABLE IF EXISTS ext_cdrdm.tmp_ALS_IND_tbl;

CREATE TABLE IF NOT EXISTS ext_cdrdm.tmp_ALS_IND_tbl AS 
-- Define table properties
SELECT subscriber_no, BAN, ALS_IND, 1 AS tbl_priority 
FROM (
  SELECT usg.subscriber_no, usg.BAN, subscr1.ALS_IND, 
         ROW_NUMBER() OVER(PARTITION BY usg.subscriber_no, usg.BAN ORDER BY subscr1.SYS_CREATION_DATE DESC) AS latest_ALS_IND
  FROM ( SELECT subscriber_no, BAN, channel_seizure_dt FROM ods_usage.usage ) usg 
  JOIN (
        SELECT subscriber_no, customer_BAN, ALS_IND, EFFECTIVE_DATE, EXPIRATION_DATE, SYS_CREATION_DATE
        FROM ELA_V21.subscriber_history
        WHERE subscriber_no IS NOT NULL
        AND customer_BAN IS NOT NULL 
        AND ALS_IND IN ('P','H','U','V')        
   ) subscr1
   ON usg.subscriber_no = subscr1.subscriber_no
   AND CAST(usg.BAN AS decimal(10,0)) = subscr1.customer_BAN
   WHERE usg.channel_seizure_dt BETWEEN subscr1.EFFECTIVE_DATE AND subscr1.EXPIRATION_DATE
   AND subscr1.ALS_IND IS NOT NULL
 ) sub_hist
WHERE latest_ALS_IND=1

UNION ALL

SELECT subscriber_no, BAN, ALS_IND, 2 AS tbl_priority 
FROM (
  SELECT usg.subscriber_no, usg.BAN, subscr2.ALS_IND,
         ROW_NUMBER() OVER(PARTITION BY usg.subscriber_no, usg.BAN ORDER BY subscr2.SYS_UPDATE_DATE DESC) AS latest_ALS_IND  
  FROM ( SELECT subscriber_no, BAN, channel_seizure_dt FROM ods_usage.usage ) usg
  JOIN (
      SELECT subscriber_no, customer_BAN, ALS_IND, sub_status, EFFECTIVE_DATE, SYS_UPDATE_DATE
      FROM ELA_V21.subscriber
      WHERE subscriber_no IS NOT NULL
      AND customer_BAN IS NOT NULL
      AND ALS_IND IN ('P','H','U','V')
   ) subscr2
   ON usg.subscriber_no = subscr2.subscriber_no
   -- Check Production tables and remove CASTing if both Types are same
   AND CAST(usg.BAN AS decimal(10,0)) = subscr2.customer_BAN
   WHERE TO_DATE(usg.channel_seizure_dt) >= TO_DATE(subscr2.EFFECTIVE_DATE)
   AND subscr2.sub_status = 'A'
 ) subscriber
WHERE latest_ALS_IND=1

UNION ALL

SELECT subscriber_no, BAN, feature_code AS ALS_IND, 3 AS tbl_priority FROM (
  SELECT usg.subscriber_no, usg.BAN, subscr3.feature_code,
        ROW_NUMBER() OVER(PARTITION BY usg.subscriber_no, usg.BAN ORDER BY subscr3.SYS_CREATION_DATE DESC) AS latest_ALS_IND
  FROM ( SELECT subscriber_no, BAN, channel_seizure_dt FROM ods_usage.usage ) usg
  JOIN (
        SELECT subscriber_no, BAN, feature_code, FTR_EFFECTIVE_DATE, FTR_EXPIRATION_DATE, SYS_CREATION_DATE
        FROM ELA_V21.srv_ftr_history
        WHERE subscriber_no IS NOT NULL
        AND BAN IS NOT NULL
        AND feature_code = 'UCCUSR'    
   ) subscr3
   ON usg.subscriber_no = subscr3.subscriber_no
   AND usg.BAN = subscr3.BAN
   WHERE usg.channel_seizure_dt BETWEEN subscr3.FTR_EFFECTIVE_DATE AND subscr3.FTR_EXPIRATION_DATE
   AND subscr3.feature_code IS NOT NULL   
 ) ftr_hist
WHERE latest_ALS_IND=1

UNION ALL

SELECT subscriber_no, BAN, feature_code AS ALS_IND, 4 AS tbl_priority FROM (
  SELECT usg.subscriber_no, usg.BAN, subscr4.feature_code,
         ROW_NUMBER() OVER(PARTITION BY usg.subscriber_no, usg.BAN ORDER BY subscr4.SYS_CREATION_DATE DESC) AS latest_ALS_IND
  FROM ( SELECT subscriber_no, BAN, channel_seizure_dt FROM ods_usage.usage ) usg
  JOIN (
        SELECT subscriber_no, BAN, feature_code, FTR_EFFECTIVE_DATE, FTR_EXPIRATION_DATE, SYS_CREATION_DATE
        FROM ELA_V21.service_feature
        WHERE feature_code = 'UCCUSR' 
        AND subscriber_no IS NOT NULL
        AND BAN IS NOT NULL 
   ) subscr4
   ON usg.subscriber_no = subscr4.subscriber_no
   AND usg.BAN = subscr4.BAN
   WHERE usg.channel_seizure_dt BETWEEN subscr4.FTR_EFFECTIVE_DATE AND COALESCE(subscr4.FTR_EXPIRATION_DATE,'2099-12-31')
   AND subscr4.feature_code IS NOT NULL   
 ) hist
WHERE latest_ALS_IND=1
;
---------------------------------------------------------------------------------------------

INSERT INTO TABLE ods_usage.usage PARTITION(cycle_run_year, cycle_run_month, cycle_code) 
SELECT usg.usage_id
,usg.ban
,usg.bill_seq_no
,usg.subscriber_no
,usg.channel_seizure_dt
,usg.message_switch_id
,usg.call_to_tn
,usg.sys_creation_date
,usg.sys_update_date
,usg.operator_id
,usg.application_id
,usg.dl_service_code
,usg.dl_update_stamp
,usg.soc
,usg.soc_date
,usg.currency_code
,usg.feature_code
,usg.rate_eff_date
,usg.rated_ftr_lvl
,usg.rating_level_code
,usg.price_plan_code
,usg.service_ftr_seq_no
,usg.call_action_code
,usg.rate_action_code
,usg.feature_selection_dt
,usg.product_type
,usg.bl_prsnt_ftr_cd
,usg.rating_fu_type
,usg.unit_esn
,usg.ring_time
,usg.recorded_units
,usg.recorded_units_uom
,usg.rounded_units
,usg.units_to_rate
,usg.num_of_iu
,usg.charge_amt
,usg.charge_amt_foreign
,usg.charge_amt_incl_iu
,usg.call_start_period
,usg.call_end_period
,usg.call_term_code
,usg.call_to_city_desc
,usg.call_from_state_code
,usg.call_to_state_code
,usg.orig_v_coordinate
,usg.orig_h_coordinate
,usg.term_v_coordinate
,usg.term_h_coordinate
,usg.bill_presentation_no
,usg.serve_sid
,usg.serve_cell
,usg.serve_province
,usg.serve_place
,usg.home_sid
,usg.home_province
,usg.mps_file_number
,usg.toll_rate_period
,usg.toll_type
,usg.mps_ld_feature_code
,usg.toll_charge_amt
,usg.toll_charge_amt_foreign
,usg.toll_dur_sec
,usg.toll_dur_round_min
,usg.special_num_type
,usg.market_code
,usg.sub_market_code
,usg.orig_air_occ_charge
,usg.orig_air_occ_tax
,usg.incollect_rerate_ind
,usg.message_type
,usg.ac_free_code
,usg.ac_amt
,usg.ac_amt_foreign
,usg.ac_soc
,usg.ac_soc_date
,usg.ac_currency_code
,usg.rm_tax_amt_toll
,usg.rm_tax_amt_air
,usg.rm_tax_amt_ac
,usg.tax_province
,usg.tax_wave_ind
,usg.toll_tax_waive_ind
,usg.charge_free_code
,usg.toll_free_code
,usg.call_direction_code
,usg.call_type_feature
,usg.call_cross_dt_ind
,usg.call_cross_prd_ind
,usg.call_cross_stp_ind
,usg.call_cross_im_ind
,usg.cancel_record_ind
,usg.call_adjustment_ind
,usg.bl_rerate_code
,usg.bl_rerate_date
,usg.activity_resolve_cd
,usg.mps_feature_code_1
,usg.mps_feature_code_2
,usg.mps_feature_code_3
,usg.mps_feature_code_4
,usg.mps_feature_code_5
,usg.fuyt_ind
,usg.roam_soc
,usg.rate_group
,usg.extended_hm_area_ind
,usg.price_plan_eff_date
,usg.soc_seq_no
,usg.roam_soc_eff_dt
,usg.roam_soc_rt_eff_dt
,usg.toll_soc
,usg.toll_soc_seq_no
,usg.toll_currency_code
,usg.toll_feature_code
,usg.toll_rate_action_code
,usg.toll_serv_ftr_seq_no
,usg.toll_soc_date
,usg.toll_rate_eff_date
,usg.toll_rating_level
,usg.toll_rated_ftr_lvl
,usg.toll_num_mins_to_rate
,usg.toll_num_of_im
,usg.toll_start_prd
,usg.toll_chrg_amt_incl_im
,usg.free_units_ind
,usg.num_of_free_units
,usg.format_source
,usg.record_id
,usg.in_route
,usg.out_route
,usg.orig_toll_charge
,usg.orig_toll_tax
,usg.rjct_sending_sid
,usg.rjct_seq
,usg.rjct_rrc
,usg.outcol_source
,usg.extended_grace_ind
,usg.untis_to_rate_uom
,usg.toll_uom_code
,usg.exact_pricing_ind
,usg.exact_pric_soc
,usg.exact_pric_feature
,usg.exact_pric_eff_date
,usg.exact_pric_action_code
,usg.ban_level_1
,usg.ban_level_2
,usg.ban_level_3
,usg.fict_sid
,usg.ems_counter
,usg.ac_feature_code
,usg.ac_rate_eff_date
,usg.ac_rate_action_code
,usg.rate
,usg.ac_serv_ftr_seq_no
,usg.ac_soc_seq_no
,usg.imsi
,usg.guide_by
,usg.call_to_imsi
,usg.call_to_type
,usg.serve_plmn
,usg.home_plmn
,usg.product_sub_type
,usg.utc_offset
,usg.cdr_source_id
,usg.special_fm_eligibility
,usg.mms_agent
,usg.number_of_recipients
,usg.mms_type
,usg.mms_sub_type
,usg.bearer_channel
,usg.mms_class
,usg.mms_delivery_type
,usg.toll_rating_fu_type
,usg.ps_air
,usg.ps_toll
,usg.sur_rur_id
,usg.sur_process_date
,usg.sur_report_id
,usg.record_type_ind
,usg.apn
,usg.serve_country_cd
,usg.in_ind
,usg.ld_rate
,usg.mps_ld_feature_code_1
,usg.mps_ld_feature_code_2
,usg.jv_site
,usg.us_stream_type
,usg.dom_fallback1
,usg.dom_fallback2
,usg.surchrg_soc
,usg.surchrg_ftr
,usg.etl_created_workflow_run_id
,usg.etl_created_ts
,usg.gg_src_dbname
,usg.gg_op_type
,usg.gg_commit_ts
,usg.gg_replicat_name
,usg.gg_load_to_tdat_ts
,usg.gg_load_sequence
,usg.gg_log_rba
,usg.gg_source_ts
,usg.gg_bigdata_scn
,usg.gg_bigdata_op_type
,usg.gg_bigdata_log_position
,usg.gg_datalake_load_ts
,mps.ACCOUNT_SUB_TYPE AS tr_account_sub_type
,mps.COMPANY_CODE AS tr_company_code
,mps.FRANCHISE_CD AS tr_franchise_cd
,mps.TR_BA_ACCOUNT_TYPE
,tmp.ALS_IND AS tr_sub_ucc_ind  
,usg.sqoop_ext_date
,from_unixtime(unix_timestamp()) AS insert_ts
,usg.cycle_run_year
,usg.cycle_run_month
,usg.cycle_code
FROM ext_ods_usage.usage usg
LEFT OUTER JOIN (
      SELECT subscriber_no, BAN, ACCOUNT_SUB_TYPE,COMPANY_CODE, FRANCHISE_CD, TR_BA_ACCOUNT_TYPE   
       FROM cdrdm.DIM_MPS_CUST_1
       WHERE MPS_CUST_SEQ_NO = 1
       AND product_type = 'C') mps
ON usg.subscriber_no = mps.subscriber_no
AND usg.BAN = mps.BAN
-------------- joining TEMP table to get ALS_IND -----------------

LEFT OUTER JOIN (SELECT subscriber_no, BAN, ALS_IND 
                 FROM (SELECT subscriber_no, BAN, ALS_IND, ROW_NUMBER() OVER(PARTITION BY subscriber_no, BAN ORDER BY tbl_priority ASC) AS prirt
                       FROM ext_cdrdm.tmp_ALS_IND_tbl) tmp_ALS
                       WHERE prirt=1) tmp
ON usg.subscriber_no = tmp.subscriber_no
AND usg.BAN = tmp.BAN
-------------------------------
;

DROP TABLE IF EXISTS ext_cdrdm.tmp_ALS_IND_tbl;