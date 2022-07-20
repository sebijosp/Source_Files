-- Model attributes changes in base snapshot
--alter table data_pods.wl_base_snapshot_sub_summary_dly change ppc_model_probability c_ppc_model_probability double;
--alter table data_pods.wl_base_snapshot_sub_summary_dly change ppc_model_rp c_ppc_model_rp double;
--alter table data_pods.wl_base_snapshot_sub_summary_dly change hup_model_probability c_hup_model_probability  double;
--alter table data_pods.wl_base_snapshot_sub_summary_dly change hup_model_rp c_hup_model_rp double;
--alter table data_pods.wl_base_snapshot_sub_summary_dly change churn_vc_model_probability c_churn_vc_model_probability double;
--alter table data_pods.wl_base_snapshot_sub_summary_dly change churn_vc_model_rp c_churn_vc_model_rp double;
--alter table data_pods.wl_base_snapshot_sub_summary_dly add columns(o_ppc_model_probability double, o_ppc_model_rp double, o_hup_model_probability  double, o_hup_model_rp double, o_churn_vc_model_probability double, o_churn_vc_model_rp double);

-- DNC attributes addition in base snapshot
alter table data_pods.wl_base_snapshot_sub_summary_dly change o_activation_date activation_date timestamp;
alter table data_pods.wl_base_snapshot_sub_summary_dly change o_ban_min_activation_date ban_min_activation_date date;
alter table data_pods.wl_base_snapshot_sub_summary_dly change ppc_request_date c_ppc_request_date timestamp;
alter table data_pods.wl_base_snapshot_sub_summary_dly change ppc_eff_date c_ppc_eff_date timestamp;
alter table data_pods.wl_base_snapshot_sub_summary_dly change hup_request_date c_hup_request_date timestamp;
alter table data_pods.wl_base_snapshot_sub_summary_dly change hup_status_date c_hup_status_date timestamp;
alter table data_pods.wl_base_snapshot_sub_summary_dly add columns(o_ppc_request_date timestamp, o_ppc_eff_date timestamp, o_hup_request_date timestamp, o_hup_status_date timestamp, OPTOUT_DM_FLAG int, OPTOUT_EM_FLAG int,OPTOUT_SMS_FLAG int, OPTOUT_TM_FLAG int,SUPER_OPTOUT_FLAG int,MKT_FLAG int);
