set hive.execution.engine=tez;
use app_customer_journey;

create table if not exists hist_ecid_ods_billing_account
(customer_ban decimal(9,0)
,franchise_tp char(1)
,ecid bigint
,hash_ban varchar(300)
,crm_acct_type varchar(100)
,crm_acct_subtype varchar(100))
partitioned by (build_date date) stored as orc;

create table if not exists ecid_ods_billing_account
(customer_ban decimal(9,0)
,franchise_tp char(1)
,ecid bigint
,hash_ban varchar(300)
,crm_acct_type varchar(100)
,crm_acct_subtype varchar(100))
stored as orc;

--create table if not exists hist_ecid_ods_subscriber
--(customer_id decimal(9,0)
--,subscriber_no varchar(20)
--,effective_date timestamp
--,expiration_date timestamp
--,init_activation_date timestamp
--,deactivation_date timestamp
--,deact_reason_cd char(4)
--,sub_status char(1)
--,sub_status_date timestamp
--,sub_status_last_act char(3)
--,sub_status_rsn_code char(4)
--,sub_status_count decimal(1,0)
--,product_type char(1)
--,customer_ban decimal(9,0)
--,bill_cycle decimal(2,0)
--,channel_id char(5)
--,tax_stt_exmp_ind char(1)
--,tax_fed_exmp_ind char(1)
--,tax_rm_exmp_ind char(1)
--,tax_stt_exmp_date timestamp
--,tax_fed_exmp_date timestamp
--,tax_rm_exmp_date timestamp
--,tax_stt_ref_no char(15)
--,tax_fed_ref_no char(15)
--,tax_exmp_ren_ind char(1)
--,req_st_grace_period timestamp
--,req_end_grace_period timestamp
--,commit_start_date timestamp
--,commit_end_date timestamp
--,commit_reason_code char(3)
--,commit_orig_no_month decimal(3,0)
--,susp_rc_rate_type char(1)
--,sub_group_no decimal(3,0)
--,req_deposit_amt decimal(11,2)
--,next_ctn varchar(20)
--,next_ctn_chg_date timestamp
--,prv_ctn varchar(20)
--,prv_ctn_chg_date timestamp
--,next_ban decimal(9,0)
--,next_ban_move_date timestamp
--,prv_ban decimal(9,0)
--,prv_ban_move_date timestamp
--,sub_sts_issue_date timestamp
--,activate_waive_rsn char(6)
--,earliest_actv_date timestamp
--,spanc_code char(3)
--,sub_actv_location char(4)
--,contact_tn_num char(10)
--,contact_tn_ext char(5)
--,init_com_dlr char(5)
--,init_com_split_dlr char(5)
--,ctn_seq_no decimal(9,0)
--,pic_billing_in char(10)
--,pic_customer_type char(2)
--,contract_type char(1)
--,cs_vip_class char(10)
--,vip_count decimal(1,0)
--,sub_feature_lang char(2)
--,bill_language char(2)
--,npa varchar(3)
--,nxx varchar(3)
--,region char(2)
--,live_nonlive char(1)
--,live_count decimal(1,0)
--,months_service decimal(3,0)
--,source_creator_cd char(3)
--,price_plan varchar(9)
--,soc_effective_date timestamp
--,user_id decimal(9,0)
--,n_in_1 char(1)
--,sub_market_cd char(3)
--,esn_type char(3)
--,load_date timestamp
--,dept_code char(9)
--,deposit_amount decimal(9,2)
--,prepayment_amount decimal(9,2)
--,rc_freq decimal(2,0)
--,company_code char(9)
--,account_type char(1)
--,account_sub_type char(1)
--,bsn varchar(20)
--,sub_birth_date timestamp
--,decl_birth_info_ind char(1)
--,act_channel char(1)
--,franchise_cd char(3)
--,franchise_tp char(1)
--,pcount decimal(2,0)
--,sys_creation_date timestamp
--,sys_update_date timestamp
--,msid decimal(8,0)
--,tfn_terminating_no varchar(20)
--,pi_subscriber_no varchar(20)
--,port_freeze_ind char(1)
--,key_activation_date timestamp
--,als_ind char(1)
--,sub_type char(1)
--,wifi_ind char(1)
--,payee_ind char(1)
--,netwrk char(1)
--,datalake_execution_id bigint
--,datalake_load_ts timestamp
--,ecid bigint
--,hash_ban varchar(300)
--,crm_acct_type varchar(100)
--,crm_acct_subtype varchar(100))
--partitioned by (build_date date) stored as orc;
--
--create table if not exists ecid_ods_subscriber
--(customer_id decimal(9,0)
--,subscriber_no varchar(20)
--,effective_date timestamp
--,expiration_date timestamp
--,init_activation_date timestamp
--,deactivation_date timestamp
--,deact_reason_cd char(4)
--,sub_status char(1)
--,sub_status_date timestamp
--,sub_status_last_act char(3)
--,sub_status_rsn_code char(4)
--,sub_status_count decimal(1,0)
--,product_type char(1)
--,customer_ban decimal(9,0)
--,bill_cycle decimal(2,0)
--,channel_id char(5)
--,tax_stt_exmp_ind char(1)
--,tax_fed_exmp_ind char(1)
--,tax_rm_exmp_ind char(1)
--,tax_stt_exmp_date timestamp
--,tax_fed_exmp_date timestamp
--,tax_rm_exmp_date timestamp
--,tax_stt_ref_no char(15)
--,tax_fed_ref_no char(15)
--,tax_exmp_ren_ind char(1)
--,req_st_grace_period timestamp
--,req_end_grace_period timestamp
--,commit_start_date timestamp
--,commit_end_date timestamp
--,commit_reason_code char(3)
--,commit_orig_no_month decimal(3,0)
--,susp_rc_rate_type char(1)
--,sub_group_no decimal(3,0)
--,req_deposit_amt decimal(11,2)
--,next_ctn varchar(20)
--,next_ctn_chg_date timestamp
--,prv_ctn varchar(20)
--,prv_ctn_chg_date timestamp
--,next_ban decimal(9,0)
--,next_ban_move_date timestamp
--,prv_ban decimal(9,0)
--,prv_ban_move_date timestamp
--,sub_sts_issue_date timestamp
--,activate_waive_rsn char(6)
--,earliest_actv_date timestamp
--,spanc_code char(3)
--,sub_actv_location char(4)
--,contact_tn_num char(10)
--,contact_tn_ext char(5)
--,init_com_dlr char(5)
--,init_com_split_dlr char(5)
--,ctn_seq_no decimal(9,0)
--,pic_billing_in char(10)
--,pic_customer_type char(2)
--,contract_type char(1)
--,cs_vip_class char(10)
--,vip_count decimal(1,0)
--,sub_feature_lang char(2)
--,bill_language char(2)
--,npa varchar(3)
--,nxx varchar(3)
--,region char(2)
--,live_nonlive char(1)
--,live_count decimal(1,0)
--,months_service decimal(3,0)
--,source_creator_cd char(3)
--,price_plan varchar(9)
--,soc_effective_date timestamp
--,user_id decimal(9,0)
--,n_in_1 char(1)
--,sub_market_cd char(3)
--,esn_type char(3)
--,load_date timestamp
--,dept_code char(9)
--,deposit_amount decimal(9,2)
--,prepayment_amount decimal(9,2)
--,rc_freq decimal(2,0)
--,company_code char(9)
--,account_type char(1)
--,account_sub_type char(1)
--,bsn varchar(20)
--,sub_birth_date timestamp
--,decl_birth_info_ind char(1)
--,act_channel char(1)
--,franchise_cd char(3)
--,franchise_tp char(1)
--,pcount decimal(2,0)
--,sys_creation_date timestamp
--,sys_update_date timestamp
--,msid decimal(8,0)
--,tfn_terminating_no varchar(20)
--,pi_subscriber_no varchar(20)
--,port_freeze_ind char(1)
--,key_activation_date timestamp
--,als_ind char(1)
--,sub_type char(1)
--,wifi_ind char(1)
--,payee_ind char(1)
--,netwrk char(1)
--,datalake_execution_id bigint
--,datalake_load_ts timestamp
--,ecid bigint
--,hash_ban varchar(300)
--,crm_acct_type varchar(100)
--,crm_acct_subtype varchar(100))
--stored as orc;
