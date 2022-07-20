--[Version History]
--0.1 - danchang - 3/17/2016 - updated logic based on reference SQL provided in "Semantic Layer (parts of SQL) v1.txt"
--
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
SET hive.tez.log.level=DEBUG;

CREATE VIEW IF NOT EXISTS cdrdm.v_ADJ_Info_UCC AS
SELECT
a.ban,
a.ent_seq_no,
a.actv_seq_no,
a.sys_creation_date,
a.sys_update_date,
a.operator_id,
a.application_id,
a.dl_service_code,
a.dl_update_stamp,
a.actv_code,
a.actv_reason_code,
a.actv_date,
a.actv_amt,
a.actv_bill_seq_no,
a.adj_creation_date,
a.effective_date,
a.inv_seq_no,
a.balance_impact_code,
a.adj_level_code,
a.source_appl_code,
a.priod_cvrg_st_date,
a.priod_cvrg_nd_date,
a.memo_id,
a.charge_seq_no,
a.subscriber_no,
a.sub_market_cd,
a.soc,
a.feature_code,
a.ftr_revenue_code,
a.service_ftr_seq_no,
a.source_bill_seq_no,
a.tax_gst_amt,
a.tax_pst_amt,
a.tax_hst_amt,
a.tax_qst_amt,
a.tax_gst_taxbl_amt,
a.tax_pst_taxbl_amt,
a.tax_hst_taxbl_amt,
a.tax_qst_taxbl_amt,
a.tax_pst_detail_ind,
a.tax_roaming_amt,
a.tax_commission_ind,
a.discount_cd,
a.channel_seizure_dt,
a.message_switch_id,
a.message_type,
a.msg_exist_ind,
a.at_call_dur_rond_min,
a.at_call_start_period,
a.at_call_end_period,
a.product_type,
a.call_to_city_desc,
a.call_to_state_code,
a.call_to_tn,
a.call_to_prefix,
a.call_type_feature,
a.call_action_code,
a.unit_esn,
a.bill_comment,
a.adj_group_id,
a.conv_run_no,
a.manual_override_ind,
a.soc_seq_no,
a.show_on_bill_ind,
a.bl_ignore_ind,
a.charge_type,
a.asgn_to_bill_appl_cd,
a.discount_pct,
a.discount_from_amt,
a.discount_category,
a.adj_group_level,
a.finance_eff_date,
a.rms_inv_store_id,
a.rms_inv_actv_type,
a.rms_inv_id,
a.quantity,
a.designation,
a.inv_settelment_id,
a.chg_group_id,
a.bl_adj_date,
a.bl_year,
a.bl_month,
a.adj_bill_seq_no,
a.utc_ind,
a.utc_to_ban,
a.feature_category,
a.utc_from_ban,
a.utc_from_subscriber,
a.utc_discount_type,
a.usage_ind,
a.utc_seq_no,
a.franchise_cd,
a.waiving_code,
a.tax_hst_detail_ind,
a.eqsub_seq_no,
a.gg_src_dbname,
a.gg_op_type,
a.gg_commit_ts,
a.gg_replicat_name,
a.gg_load_to_tdat_ts,
a.gg_load_sequence,
a.gg_log_rba,
a.gg_source_ts,
a.m2m_ind,
a.gg_bigdata_scn,
a.gg_bigdata_op_type,
a.gg_bigdata_log_position,
a.gg_datalake_load_ts,
b.reason_code AS rsn_reason_code,
b.sys_creation_date AS rsn_sys_creation_date,
b.sys_update_date AS rsn_sys_update_date,
b.operator_id AS rsn_operator_id,
b.application_id AS rsn_application_id,
b.dl_service_code AS rsn_dl_service_code,
b.dl_update_stamp AS rsn_dl_update_stamp,
b.adj_category AS rsn_adj_category,
b.adj_type_code AS rsn_adj_type_code,
b.bl_incl_prst_ind AS rsn_bl_incl_prst_ind,
b.bl_txt_ovr_ind AS rsn_bl_txt_ovr_ind,
b.adj_def_amt AS rsn_adj_def_amt,
b.adj_def_amt_over_ind AS rsn_adj_def_amt_over_ind,
b.adj_tax_rep_ind AS rsn_adj_tax_rep_ind,
b.adj_tax_credit_code AS rsn_adj_tax_credit_code,
b.adj_level_code AS rsn_adj_level_code,
b.adj_actv_code AS rsn_adj_actv_code,
b.man_adj_ind AS rsn_man_adj_ind,
b.inv_type AS rsn_inv_type,
b.adj_short_dsc AS rsn_adj_short_dsc,
b.adj_dsc AS rsn_adj_dsc,
b.dc_red_ind AS rsn_dc_red_ind,
b.sec_class AS rsn_sec_class,
b.process_code AS rsn_process_code,
b.tolerance_ind AS rsn_tolerance_ind,
b.adj_dsc_f AS rsn_adj_dsc_f,
b.adj_short_dsc_f AS rsn_adj_short_dsc_f,
b.specific_adj_ind AS rsn_specific_adj_ind,
b.adj_max_amt AS rsn_adj_max_amt,
b.from_activation_date AS rsn_from_activation_date,
b.to_activation_date AS rsn_to_activation_date,
b.clm_ind AS rsn_clm_ind,
b.all_price_plan_ind AS rsn_all_price_plan_ind,
b.all_company_ind AS rsn_all_company_ind,
b.all_ban_type_ind AS rsn_all_ban_type_ind,
b.all_npa_nxx_ind AS rsn_all_npa_nxx_ind,
b.all_cycle_ind AS rsn_all_cycle_ind,
b.all_product_ind AS rsn_all_product_ind,
b.all_credit_class_ind AS rsn_all_credit_class_ind,
b.all_ban_status_ind AS rsn_all_ban_status_ind,
b.recoverable_ind AS rsn_recoverable_ind,
b.tab_elig_ind AS rsn_tab_elig_ind,
b.credit_term AS rsn_credit_term,
b.credit_type AS rsn_credit_type,
b.all_activity_ind AS rsn_all_activity_ind,
b.all_dealer_ind AS rsn_all_dealer_ind,
b.from_msf AS rsn_from_msf,
b.to_msf AS rsn_to_msf,
b.gg_src_dbname AS rsn_gg_src_dbname,
b.gg_op_type AS rsn_gg_op_type,
b.gg_commit_ts AS rsn_gg_commit_ts,
b.gg_replicat_name AS rsn_gg_replicat_name,
b.gg_load_to_tdat_ts AS rsn_gg_load_to_tdat_ts,
b.gg_load_sequence AS rsn_gg_load_sequence,
b.gg_log_rba AS rsn_gg_log_rba,
b.gg_source_ts AS rsn_gg_source_ts,
b.gg_bigdata_scn AS rsn_gg_bigdata_scn,
b.gg_bigdata_op_type AS rsn_gg_bigdata_op_type,
b.gg_bigdata_log_position AS rsn_gg_bigdata_log_position,
b.gg_datalake_load_ts AS rsn_gg_datalake_load_ts,
c.bill_conf_status AS bill_conf_status,
c.cycle_code AS cycle_code,
c.cycle_run_year AS cycle_run_year,
c.cycle_run_month AS cycle_run_month
FROM ela_v21.adjustment a JOIN ela_v21.adjustment_reason_gg b ON a.actv_reason_code = b.reason_code
LEFT OUTER JOIN
(SELECT bill_conf_status, cycle_code, cycle_run_year, cycle_run_month, ban, bill_seq_no 
  FROM ela_v21.bill) c 
  ON a.ban = c.ban 
  AND a.actv_bill_seq_no = c.bill_seq_no
LEFT OUTER JOIN 
(SELECT tab_type, EFFECTIVE_DATE, EXPIRATION_DATE, ban, subscriber_no
  FROM cdrdm.dim_ucc_sub_info
  WHERE UPPER(TRIM(tab_type)) IN ('FTR_HIST','FTR')
  ) d 
  ON a.ban = d.ban 
  AND a.subscriber_no = d.subscriber_no
WHERE a.ADJ_CREATION_DATE BETWEEN d.EFFECTIVE_DATE AND COALESCE(d.EXPIRATION_DATE,'9999-12-31:00:00:00');
