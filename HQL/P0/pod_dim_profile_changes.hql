--For CTN table:
--1)Take bkp
alter table data_pods.pod_dim_profile_ctn_dly rename to data_pods.pod_dim_profile_ctn_dly_bkp;
--2)Drop if already exist and create table by using same DDL excluding drop cols
drop table data_pods.pod_dim_profile_ctn_dly;
CREATE TABLE data_pods.pod_dim_profile_ctn_dly(
    ecid bigint,
    ban bigint,
    subscriber_no bigint,
    sub_status string,
    init_activation_date timestamp,
    ctn_tenure int,
    bill_year int,
    bill_month int,
    cycle_start_date timestamp,
    cycle_close_date timestamp,
    rogers_payer_segment_ctn varchar(70),
    rogers_payer_segment_arpu_delta double,
    market_province string,
    hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");
--3)Insert data to bck to new table
insert overwrite table data_pods.pod_dim_profile_ctn_dly partition(calendar_year, calendar_month)
select
    ecid,
    ban,
    subscriber_no,
    sub_status,
    init_activation_date,
    ctn_tenure,
    bill_year,
    bill_month,
    cycle_start_date,
    cycle_close_date,
    rogers_payer_segment_ctn,
    rogers_payer_segment_arpu_delta,
    NULL as market_province,
    hdp_update_ts,
    calendar_year,
    calendar_month
from data_pods.pod_dim_profile_ctn_dly_bkp;
--4)drop bkp table
drop table data_pods.pod_dim_profile_ctn_dly_bkp;


--Procedure for Drop column from already existing Hive partitioned table
--1)Take bkp
alter table data_pods.pod_dim_profile_ban_dly rename to data_pods.pod_dim_profile_ban_dly_bkp;

--2)Drop if already exist and create table by using same DDL excluding drop cols
drop table data_pods.pod_dim_profile_ban_dly;
CREATE TABLE data_pods.pod_dim_profile_ban_dly(
    ecid bigint,
    ban bigint,
    contact_id string,
    brand string,
    account_type  string,
    account_sub_type string,
    SEGMENT_ID string,
    segment_desc string,
    COMPANY_CODE string,
    birth_year int,
    --MARKET_PROVINCE string,
    BILL_PROVINCE string,
    BILL_CITY string,
    BILL_POSTAL_CODE string,
    ar_balance double,
    bl_last_pay_date timestamp,
    COL_DELINQ_STATUS string,
    BAN_STATUS string,
    CREDIT_CLASS string,
    idv_ind smallint,
    evp_ind smallint,
    lst_init_activation_date timestamp,
    ban_init_activation_date timestamp,
    ban_tenure int,
    bill_year int,
    bill_month int,
    cycle_start_date timestamp,
    cycle_close_date timestamp,
    ban_cnt_sub_lines double,
    anchor_segment varchar(40),
    rogers_payer_segment_ban varchar(70),
    rogers_payer_segment_arpa_delta double,
    dao_segment varchar(1000),
    hdp_update_ts timestamp)
PARTITIONED BY (
  calendar_year int,
  calendar_month int)
tblproperties ("orc.compress"="SNAPPY");


--3)Insert data to bck to new table
Insert overwrite table data_pods.pod_dim_profile_ban_dly partition(calendar_year, calendar_month)
select 
ecid,
ban,
contact_id,
brand,
account_type,
account_sub_type,
SEGMENT_ID,
segment_desc,
COMPANY_CODE,
birth_year,
BILL_PROVINCE,
BILL_CITY,
BILL_POSTAL_CODE,
ar_balance,
bl_last_pay_date,
COL_DELINQ_STATUS,
BAN_STATUS,
CREDIT_CLASS,
idv_ind,
evp_ind,
lst_init_activation_date,
ban_init_activation_date,
ban_tenure,
bill_year,
bill_month,
cycle_start_date,
cycle_close_date,
ban_cnt_sub_lines,
anchor_segment,
rogers_payer_segment_ban,
rogers_payer_segment_arpa_delta,
dao_segment,
hdp_update_ts,
calendar_year,
calendar_month
from data_pods.pod_dim_profile_ban_dly_bkp;

--4)drop bkp table
drop table data_pods.pod_dim_profile_ban_dly_bkp;

--5)describe  table
desc data_pods.pod_dim_profile_ban_dly;

