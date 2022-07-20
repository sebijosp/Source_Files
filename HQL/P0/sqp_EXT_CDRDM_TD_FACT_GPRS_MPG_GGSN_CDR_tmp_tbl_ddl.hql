CREATE TABLE ext_cdrdm.td_fact_gprs_mpg_ggsn_cdr_sqp_imp(
  record_type_ind int,
  served_imsi varchar(20),
  ggsn_address varchar(40),
  charging_id varchar(12),
  sgsn_address_1 varchar(40),
  sgsn_address_2 varchar(40),
  sgsn_address_3 varchar(40),
  sgsn_address_4 varchar(40),
  sgsn_address_5 varchar(40),
  access_point_name_ni varchar(80),
  pdp_type varchar(60),
  served_pdp_address varchar(40),
  dynamic_address_flag varchar(10),
  qos_negotiated_1 varchar(90),
  data_volume_gprs_uplink_1 bigint,
  data_volume_gprs_downlink_1 bigint,
  change_condition_1 varchar(70),
  change_time_1 string,
  user_location_information_1 varchar(40),
  qos_negotiated_2 varchar(90),
  data_volume_gprs_uplink_2 bigint,
  data_volume_gprs_downlink_2 bigint,
  change_condition_2 varchar(70),
  change_time_2 string,
  user_location_information_2 varchar(40),
  qos_negotiated_3 varchar(90),
  data_volume_gprs_uplink_3 bigint,
  data_volume_gprs_downlink_3 bigint,
  change_condition_3 varchar(70),
  change_time_3 string,
  user_location_information_3 varchar(40),
  qos_negotiated_4 varchar(90),
  data_volume_gprs_uplink_4 bigint,
  data_volume_gprs_downlink_4 bigint,
  change_condition_4 varchar(70),
  change_time_4 string,
  user_location_information_4 varchar(40),
  qos_negotiated_5 varchar(90),
  data_volume_gprs_uplink_5 bigint,
  data_volume_gprs_downlink_5 bigint,
  change_condition_5 varchar(70),
  change_time_5 string,
  user_location_information_5 varchar(40),
  record_opening_time string,
  duration int,
  cause_for_rec_closing int,
  record_sequence_number bigint,
  node_id varchar(50),
  local_sequence_number bigint,
  apn_selection_mode int,
  served_msisdn varchar(20),
  charging_charistics varchar(20),
  ch_ch_selection_mode varchar(25),
  ims_signaling_context varchar(25),
  sgsn_plmn_identifier varchar(25),
  served_imei_sv varchar(35),
  rat_type int,
  ms_time_zone varchar(15),
  user_location_information varchar(40),
  subscriber_no bigint,
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
  insert_timestamp string,
  record_opening_date string)

  stored AS RCFile;
