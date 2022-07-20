


INSERT INTO TABLE cdrdm.fact_sms_cdr_hist_loads PARTITION (local_subscriber_date) 
SELECT  

        subscriber_no                    -- subscriber_no bigint,
      , event_record_id                    -- event_record_id smallint,
      , spid                    -- spid smallint,
      , orig_msc                    -- orig_msc varchar(30),
      , other_msisdn                    -- other_msisdn bigint,
      , orig_msc_zone_indicator                    -- orig_msc_zone_indicator smallint,
      , rate                    -- rate smallint,
      , charged_party                    -- charged_party char(1),
      , action                    -- action smallint,
      , rule                    -- rule int,
      , transaction_id                    -- transaction_id int,
      , charge_result                    -- charge_result smallint,
      , balance                    -- balance int,
      , rate_plan                    -- rate_plan smallint,
      , on_net                    -- on_net char(1),
      , originating_service_grade                    -- originating_service_grade smallint, 
      , terminating_service_grade                    -- terminating_service_grade smallint,
      , sequence_number                    -- sequence_number int,
      , local_subscriber_timestamp                    -- local_subscriber_timestamp string,
      , imsi                    -- imsi bigint,
      , ban                    -- ban bigint,
      , language_1                    -- language char(1),
      , bill_cycle_date                    -- bill_cycle_date smallint,
      , originating_subscriber_loc                    -- originating_subscriber_location char(2), note the difference in column names
      , terminating_subscriber_loc                    -- terminating_subscriber_location char(2), note the difference in column names
      , rating_rule_description_1                    -- rating_rule_description_1 varchar(30),
      , rating_rule_description_2                    -- rating_rule_description_2 varchar(30),
      , origination_eq_type                    -- origination_eq_type char(1),
      , destination_eq_type                    -- destination_eq_type char(1),
      , date_1                    -- date_1 string,
      , ocn                    -- ocn char(4),
      , other_msisdn_sc                    -- other_msisdn_sc varchar(10),
      , tr_type_omsc                    -- tr_type_omsc char(1),
      , tr_omsc_plmn                    -- tr_omsc_plmn varchar(7),
      , tr_lac_tac                    -- tr_lac_tac varchar(5),
      , tr_ecid_clid                    -- tr_ecid_clid varchar(10),
      , tr_msc_type                    -- tr_msc_type varchar(5),
      , tr_dcode_country                    -- tr_dcode_country varchar(30),
      , tr_account_sub_type                    -- tr_account_sub_type char(1),
      , tr_company_code                    -- tr_company_code varchar(15),
      , tr_franchise_cd                    -- tr_franchise_cd varchar(5),
      , tr_ba_account_type                    -- tr_ba_account_type char(1),
      , mtype                    -- mtype varchar(5),
      , audit_key  as file_name                    -- file_name string,
      , null as record_start                    -- record_start bigint,
      , null as record_end                    -- record_end bigint,
      , "manual load - 201610" as record_type                    -- record_type string,
      , null as family_name                    -- family_name string,
      , null as version_id                    -- version_id int,
      , null as file_time                    -- file_time string,
      , null as file_id                    -- file_id string,
      , null as switch_name                    -- switch_name string,
      , null as num_records                    -- num_records string,
      , unix_timestamp() as insert_timestamp                    -- insert_timestamp string,

--      , CAST(local_subscriber_timestamp AS DATE) as local_subscriber_date                    -- local_subscriber_date string
      , CAST(SUBSTR(local_subscriber_timestamp,1,10) AS STRING) AS local_subscriber_date
-- need to list out all fields as EXTERNAL tables have partition on SQOOP_EXT_DATE
FROM ext_cdrdm.fact_sms_cdr_20160630 
;
-----------------------------------------------------------------


INSERT INTO TABLE cdrdm.fact_sms_cdr_hist_loads PARTITION (local_subscriber_date) 
SELECT  

        subscriber_no                    -- subscriber_no bigint,
      , event_record_id                    -- event_record_id smallint,
      , spid                    -- spid smallint,
      , orig_msc                    -- orig_msc varchar(30),
      , other_msisdn                    -- other_msisdn bigint,
      , orig_msc_zone_indicator                    -- orig_msc_zone_indicator smallint,
      , rate                    -- rate smallint,
      , charged_party                    -- charged_party char(1),
      , action                    -- action smallint,
      , rule                    -- rule int,
      , transaction_id                    -- transaction_id int,
      , charge_result                    -- charge_result smallint,
      , balance                    -- balance int,
      , rate_plan                    -- rate_plan smallint,
      , on_net                    -- on_net char(1),
      , originating_service_grade                    -- originating_service_grade smallint, 
      , terminating_service_grade                    -- terminating_service_grade smallint,
      , sequence_number                    -- sequence_number int,
      , local_subscriber_timestamp                    -- local_subscriber_timestamp string,
      , imsi                    -- imsi bigint,
      , ban                    -- ban bigint,
      , language_1                    -- language char(1),
      , bill_cycle_date                    -- bill_cycle_date smallint,
      , originating_subscriber_loc                    -- originating_subscriber_location char(2), note the difference in column names
      , terminating_subscriber_loc                    -- terminating_subscriber_location char(2), note the difference in column names
      , rating_rule_description_1                    -- rating_rule_description_1 varchar(30),
      , rating_rule_description_2                    -- rating_rule_description_2 varchar(30),
      , origination_eq_type                    -- origination_eq_type char(1),
      , destination_eq_type                    -- destination_eq_type char(1),
      , date_1                    -- date_1 string,
      , ocn                    -- ocn char(4),
      , other_msisdn_sc                    -- other_msisdn_sc varchar(10),
      , tr_type_omsc                    -- tr_type_omsc char(1),
      , tr_omsc_plmn                    -- tr_omsc_plmn varchar(7),
      , tr_lac_tac                    -- tr_lac_tac varchar(5),
      , tr_ecid_clid                    -- tr_ecid_clid varchar(10),
      , tr_msc_type                    -- tr_msc_type varchar(5),
      , tr_dcode_country                    -- tr_dcode_country varchar(30),
      , tr_account_sub_type                    -- tr_account_sub_type char(1),
      , tr_company_code                    -- tr_company_code varchar(15),
      , tr_franchise_cd                    -- tr_franchise_cd varchar(5),
      , tr_ba_account_type                    -- tr_ba_account_type char(1),
      , mtype                    -- mtype varchar(5),
      , audit_key  as file_name                    -- file_name string,
      , null as record_start                    -- record_start bigint,
      , null as record_end                    -- record_end bigint,
      , "manual load - 201610" as record_type                    -- record_type string,
      , null as family_name                    -- family_name string,
      , null as version_id                    -- version_id int,
      , null as file_time                    -- file_time string,
      , null as file_id                    -- file_id string,
      , null as switch_name                    -- switch_name string,
      , null as num_records                    -- num_records string,
      , unix_timestamp() as insert_timestamp                    -- insert_timestamp string,

--      , CAST(local_subscriber_timestamp AS DATE) as local_subscriber_date                    -- local_subscriber_date string
      , CAST(SUBSTR(local_subscriber_timestamp,1,10) AS STRING) AS local_subscriber_date 
-- need to list out all fields as EXTERNAL tables have partition on SQOOP_EXT_DATE
FROM ext_cdrdm.fact_sms_cdr_20160731 
;
-----------------------------------------------------------------


INSERT INTO TABLE cdrdm.fact_sms_cdr_hist_loads PARTITION (local_subscriber_date) 
SELECT  

        subscriber_no                    -- subscriber_no bigint,
      , event_record_id                    -- event_record_id smallint,
      , spid                    -- spid smallint,
      , orig_msc                    -- orig_msc varchar(30),
      , other_msisdn                    -- other_msisdn bigint,
      , orig_msc_zone_indicator                    -- orig_msc_zone_indicator smallint,
      , rate                    -- rate smallint,
      , charged_party                    -- charged_party char(1),
      , action                    -- action smallint,
      , rule                    -- rule int,
      , transaction_id                    -- transaction_id int,
      , charge_result                    -- charge_result smallint,
      , balance                    -- balance int,
      , rate_plan                    -- rate_plan smallint,
      , on_net                    -- on_net char(1),
      , originating_service_grade                    -- originating_service_grade smallint, 
      , terminating_service_grade                    -- terminating_service_grade smallint,
      , sequence_number                    -- sequence_number int,
      , local_subscriber_timestamp                    -- local_subscriber_timestamp string,
      , imsi                    -- imsi bigint,
      , ban                    -- ban bigint,
      , language_1                    -- language char(1),
      , bill_cycle_date                    -- bill_cycle_date smallint,
      , originating_subscriber_loc                    -- originating_subscriber_location char(2), note the difference in column names
      , terminating_subscriber_loc                    -- terminating_subscriber_location char(2), note the difference in column names
      , rating_rule_description_1                    -- rating_rule_description_1 varchar(30),
      , rating_rule_description_2                    -- rating_rule_description_2 varchar(30),
      , origination_eq_type                    -- origination_eq_type char(1),
      , destination_eq_type                    -- destination_eq_type char(1),
      , date_1                    -- date_1 string,
      , ocn                    -- ocn char(4),
      , other_msisdn_sc                    -- other_msisdn_sc varchar(10),
      , tr_type_omsc                    -- tr_type_omsc char(1),
      , tr_omsc_plmn                    -- tr_omsc_plmn varchar(7),
      , tr_lac_tac                    -- tr_lac_tac varchar(5),
      , tr_ecid_clid                    -- tr_ecid_clid varchar(10),
      , tr_msc_type                    -- tr_msc_type varchar(5),
      , tr_dcode_country                    -- tr_dcode_country varchar(30),
      , tr_account_sub_type                    -- tr_account_sub_type char(1),
      , tr_company_code                    -- tr_company_code varchar(15),
      , tr_franchise_cd                    -- tr_franchise_cd varchar(5),
      , tr_ba_account_type                    -- tr_ba_account_type char(1),
      , mtype                    -- mtype varchar(5),
      , audit_key  as file_name                    -- file_name string,
      , null as record_start                    -- record_start bigint,
      , null as record_end                    -- record_end bigint,
      , "manual load - 201610" as record_type                    -- record_type string,
      , null as family_name                    -- family_name string,
      , null as version_id                    -- version_id int,
      , null as file_time                    -- file_time string,
      , null as file_id                    -- file_id string,
      , null as switch_name                    -- switch_name string,
      , null as num_records                    -- num_records string,
      , unix_timestamp() as insert_timestamp                    -- insert_timestamp string,

--       , CAST(local_subscriber_timestamp AS DATE) as local_subscriber_date                    -- local_subscriber_date string
      , CAST(SUBSTR(local_subscriber_timestamp,1,10) AS STRING) AS local_subscriber_date
-- need to list out all fields as EXTERNAL tables have partition on SQOOP_EXT_DATE
FROM ext_cdrdm.fact_sms_cdr_20160831 
;
-----------------------------------------------------------------


INSERT INTO TABLE cdrdm.fact_sms_cdr_hist_loads PARTITION (local_subscriber_date) 
SELECT  

        subscriber_no                    -- subscriber_no bigint,
      , event_record_id                    -- event_record_id smallint,
      , spid                    -- spid smallint,
      , orig_msc                    -- orig_msc varchar(30),
      , other_msisdn                    -- other_msisdn bigint,
      , orig_msc_zone_indicator                    -- orig_msc_zone_indicator smallint,
      , rate                    -- rate smallint,
      , charged_party                    -- charged_party char(1),
      , action                    -- action smallint,
      , rule                    -- rule int,
      , transaction_id                    -- transaction_id int,
      , charge_result                    -- charge_result smallint,
      , balance                    -- balance int,
      , rate_plan                    -- rate_plan smallint,
      , on_net                    -- on_net char(1),
      , originating_service_grade                    -- originating_service_grade smallint, 
      , terminating_service_grade                    -- terminating_service_grade smallint,
      , sequence_number                    -- sequence_number int,
      , local_subscriber_timestamp                    -- local_subscriber_timestamp string,
      , imsi                    -- imsi bigint,
      , ban                    -- ban bigint,
      , language_1                    -- language char(1),
      , bill_cycle_date                    -- bill_cycle_date smallint,
      , originating_subscriber_loc                    -- originating_subscriber_location char(2), note the difference in column names
      , terminating_subscriber_loc                    -- terminating_subscriber_location char(2), note the difference in column names
      , rating_rule_description_1                    -- rating_rule_description_1 varchar(30),
      , rating_rule_description_2                    -- rating_rule_description_2 varchar(30),
      , origination_eq_type                    -- origination_eq_type char(1),
      , destination_eq_type                    -- destination_eq_type char(1),
      , date_1                    -- date_1 string,
      , ocn                    -- ocn char(4),
      , other_msisdn_sc                    -- other_msisdn_sc varchar(10),
      , tr_type_omsc                    -- tr_type_omsc char(1),
      , tr_omsc_plmn                    -- tr_omsc_plmn varchar(7),
      , tr_lac_tac                    -- tr_lac_tac varchar(5),
      , tr_ecid_clid                    -- tr_ecid_clid varchar(10),
      , tr_msc_type                    -- tr_msc_type varchar(5),
      , tr_dcode_country                    -- tr_dcode_country varchar(30),
      , tr_account_sub_type                    -- tr_account_sub_type char(1),
      , tr_company_code                    -- tr_company_code varchar(15),
      , tr_franchise_cd                    -- tr_franchise_cd varchar(5),
      , tr_ba_account_type                    -- tr_ba_account_type char(1),
      , mtype                    -- mtype varchar(5),
      , audit_key  as file_name                    -- file_name string,
      , null as record_start                    -- record_start bigint,
      , null as record_end                    -- record_end bigint,
      , "manual load - 201610" as record_type                    -- record_type string,
      , null as family_name                    -- family_name string,
      , null as version_id                    -- version_id int,
      , null as file_time                    -- file_time string,
      , null as file_id                    -- file_id string,
      , null as switch_name                    -- switch_name string,
      , null as num_records                    -- num_records string,
      , unix_timestamp() as insert_timestamp                    -- insert_timestamp string,

--      , CAST(local_subscriber_timestamp AS DATE) as local_subscriber_date                    -- local_subscriber_date string
      , CAST(SUBSTR(local_subscriber_timestamp,1,10) AS STRING) AS local_subscriber_date
-- need to list out all fields as EXTERNAL tables have partition on SQOOP_EXT_DATE
FROM ext_cdrdm.fact_sms_cdr_20160930 
;
-----------------------------------------------------------------




INSERT INTO TABLE cdrdm.fact_sms_cdr_hist_loads PARTITION (local_subscriber_date) 
SELECT  

        subscriber_no                    -- subscriber_no bigint,
      , event_record_id                    -- event_record_id smallint,
      , spid                    -- spid smallint,
      , orig_msc                    -- orig_msc varchar(30),
      , other_msisdn                    -- other_msisdn bigint,
      , orig_msc_zone_indicator                    -- orig_msc_zone_indicator smallint,
      , rate                    -- rate smallint,
      , charged_party                    -- charged_party char(1),
      , action                    -- action smallint,
      , rule                    -- rule int,
      , transaction_id                    -- transaction_id int,
      , charge_result                    -- charge_result smallint,
      , balance                    -- balance int,
      , rate_plan                    -- rate_plan smallint,
      , on_net                    -- on_net char(1),
      , originating_service_grade                    -- originating_service_grade smallint, 
      , terminating_service_grade                    -- terminating_service_grade smallint,
      , sequence_number                    -- sequence_number int,
      , local_subscriber_timestamp                    -- local_subscriber_timestamp string,
      , imsi                    -- imsi bigint,
      , ban                    -- ban bigint,
      , language_1                    -- language char(1),
      , bill_cycle_date                    -- bill_cycle_date smallint,
      , originating_subscriber_loc                    -- originating_subscriber_location char(2), note the difference in column names
      , terminating_subscriber_loc                    -- terminating_subscriber_location char(2), note the difference in column names
      , rating_rule_description_1                    -- rating_rule_description_1 varchar(30),
      , rating_rule_description_2                    -- rating_rule_description_2 varchar(30),
      , origination_eq_type                    -- origination_eq_type char(1),
      , destination_eq_type                    -- destination_eq_type char(1),
      , date_1                    -- date_1 string,
      , ocn                    -- ocn char(4),
      , other_msisdn_sc                    -- other_msisdn_sc varchar(10),
      , tr_type_omsc                    -- tr_type_omsc char(1),
      , tr_omsc_plmn                    -- tr_omsc_plmn varchar(7),
      , tr_lac_tac                    -- tr_lac_tac varchar(5),
      , tr_ecid_clid                    -- tr_ecid_clid varchar(10),
      , tr_msc_type                    -- tr_msc_type varchar(5),
      , tr_dcode_country                    -- tr_dcode_country varchar(30),
      , tr_account_sub_type                    -- tr_account_sub_type char(1),
      , tr_company_code                    -- tr_company_code varchar(15),
      , tr_franchise_cd                    -- tr_franchise_cd varchar(5),
      , tr_ba_account_type                    -- tr_ba_account_type char(1),
      , mtype                    -- mtype varchar(5),
      , audit_key  as file_name                    -- file_name string,
      , null as record_start                    -- record_start bigint,
      , null as record_end                    -- record_end bigint,
      , "manual load - 201610" as record_type                    -- record_type string,
      , null as family_name                    -- family_name string,
      , null as version_id                    -- version_id int,
      , null as file_time                    -- file_time string,
      , null as file_id                    -- file_id string,
      , null as switch_name                    -- switch_name string,
      , null as num_records                    -- num_records string,
      , unix_timestamp() as insert_timestamp                    -- insert_timestamp string,

--      , CAST(local_subscriber_timestamp AS DATE) as local_subscriber_date                    -- local_subscriber_date string
      , CAST(SUBSTR(local_subscriber_timestamp,1,10) AS STRING) AS local_subscriber_date
-- need to list out all fields as EXTERNAL tables have partition on SQOOP_EXT_DATE
FROM ext_cdrdm.fact_sms_cdr_20161017 
;
-----------------------------------------------------------------



