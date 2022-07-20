


--Procedure for Drop column from already existing Hive partitioned table
--1)Take bkp
alter table data_pods.pod_dim_priceplan_ctn_dly rename to data_pods.pod_dim_priceplan_ctn_dly_bkp;

--2)Drop if already exist and create table by using same DDL excluding drop cols

drop table data_pods.pod_dim_priceplan_ctn_dly;
CREATE TABLE data_pods.pod_dim_priceplan_ctn_dly(
    ecid                    bigint,
    ban                     bigint,
    subscriber_no           bigint,
    pplan_series_type       smallint,
    price_plan_category     string,
    connection_type         string,
    pp_prod_type            string,
    pp_type_1               string,
    pp_type_4               string,
    pp_type_7               string,
    pp_payee                string,
    pp_code                 string,
    pp_desc                 string,
    PP_GROUP_DESC           string,
    infinite_ind            smallint,
    PULSE_IND               smallint,
    DOP_IND                 smallint,
    retention_ind           smallint,
    subsidy_category        string,
    pp_subsidy_tier         string,
    PP_CAT_CODE             string,
    plan_service_rc         double,
    plan_hardware_rc        double,
    plan_rc                 double,
    non_plan_rc             double,
    discount_rc             double,
    hardware_discount_rc    double,
    mdt_rc                  double,
    total_mrc               double,
    sub_dtu_bucket          bigint,
    sub_dtu_bonus_bucket    bigint,
    sub_total_dtu_bucket    bigint,
    sub_data_bucket_mb      bigint,
    sub_total_data_bucket_mb        bigint,
    hardware_payment_category       string,
    --hardware_payment_category_alt   string,
    ppc_year                int,
    ppc_month               int,
    ppc_eff_date            timestamp,
    ppc_exp_date            timestamp,
    ppc_sale_exp_date       timestamp,
    ppc_request_date        timestamp,
    ppc_tenure              int,
    ppc_channel_id          string,
    ppc_channel_type        string,
    hdp_update_ts           timestamp)
PARTITIONED BY ( 
  calendar_year int, 
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");





--3)Insert data to bck to new table
Insert overwrite table data_pods.pod_dim_priceplan_ctn_dly partition(calendar_year, calendar_month)
select 
    ecid,
    ban                     ,
    subscriber_no           ,
    pplan_series_type       ,
    price_plan_category     ,
    connection_type         ,
    pp_prod_type            ,
    pp_type_1               ,
    pp_type_4               ,
    pp_type_7               ,
    pp_payee                ,
    pp_code                 ,
    pp_desc                 ,
    PP_GROUP_DESC           ,
    infinite_ind            ,
    PULSE_IND               ,
    DOP_IND                 ,
    retention_ind           ,
    subsidy_category        ,
    pp_subsidy_tier         ,
    PP_CAT_CODE             ,
    plan_service_rc         ,
    plan_hardware_rc        ,
    plan_rc                 ,
    non_plan_rc             ,
    discount_rc             ,
    hardware_discount_rc    ,
    mdt_rc                  ,
    total_mrc               ,
    sub_dtu_bucket          ,
    sub_dtu_bonus_bucket    ,
    sub_total_dtu_bucket    ,
    sub_data_bucket_mb      ,
    sub_total_data_bucket_mb,
    hardware_payment_category,
    ppc_year                ,
    ppc_month               ,
    ppc_eff_date            ,
    ppc_exp_date            ,
    ppc_sale_exp_date       ,
    ppc_request_date        ,
    ppc_tenure              ,
    ppc_channel_id          ,
    ppc_channel_type        ,
    hdp_update_ts           ,
    calendar_year           , 
    calendar_month
from data_pods.pod_dim_priceplan_ctn_dly_bkp;

--4)drop bkp table
drop table data_pods.pod_dim_priceplan_ctn_dly_bkp;

--5)describe  table
desc data_pods.pod_dim_priceplan_ctn_dly;

