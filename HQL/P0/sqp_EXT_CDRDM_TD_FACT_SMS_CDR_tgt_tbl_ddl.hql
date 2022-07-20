CREATE TABLE ext_cdrdm.TD_FACT_SMS_CDR(
  subscriber_no bigint,
  event_record_id smallint,
  spid smallint,
  orig_msc varchar(30),
  other_msisdn bigint,
  orig_msc_zone_indicator smallint,
  rate smallint,
  charged_party char(1),
  action smallint,
  rule int,
  transaction_id int,
  charge_result smallint,
  balance int,
  rate_plan smallint,
  on_net char(1),
  originating_service_grade smallint,
  terminating_service_grade smallint,
  sequence_number int,
  local_subscriber_timestamp string,
  imsi bigint,
  ban bigint,
  language_1 char(1),
  bill_cycle_date smallint,
  originating_subscriber_loc char(2),
  terminating_subscriber_loc char(2),
  rating_rule_description_1 varchar(30),
  rating_rule_description_2 varchar(30),
  origination_eq_type char(1),
  destination_eq_type char(1),
  date_1 string,
  ocn char(4),
  other_msisdn_sc varchar(10),
  tr_type_omsc char(1),
  tr_omsc_plmn varchar(7),
  tr_lac_tac varchar(5),
  tr_ecid_clid varchar(10),
  tr_msc_type varchar(5),
  tr_dcode_country varchar(30),
  tr_account_sub_type char(1),
  tr_company_code varchar(15),
  tr_franchise_cd varchar(5),
  tr_ba_account_type char(1),
  mtype varchar(5),
  file_name string,
  record_start bigint,
  record_end bigint,
  record_type string,
  family_name string,
  version_id int,
  file_time string,
  file_id string,
  switch_name string,
  num_records string,
  insert_timestamp string)
PARTITIONED BY (
  local_subscriber_date string)
STORED as ORC;

