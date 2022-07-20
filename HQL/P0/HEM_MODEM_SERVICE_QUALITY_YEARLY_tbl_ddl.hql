DROP TABLE IF EXISTS HEM.MODEM_SERVICE_QUALITY_YEARLY;
CREATE TABLE `HEM.MODEM_SERVICE_QUALITY_YEARLY`(
  `mac` string COMMENT 'MAC address of Cable Modem',
  `cmts` string COMMENT 'Contains the Fully Qualified Domain Name (FQDN) of the CMTS',
  `cbu_code` string COMMENT 'CBU ',

  `account` string COMMENT 'Subscriber Account Number',
  `system` string COMMENT 'Billing System (SS or Maestro)',


  `phub` string COMMENT 'Primary HUB',
  `shub` string COMMENT 'Secondary HUB',
  `channel_number` string COMMENT 'Channel number from cmts_cm_us_ch_if_name ',
  `cmts_md_if_name` string COMMENT 'MAC Domain',
  `cmts_md_if_index` string COMMENT 'Mac Domain Index',

  `node` string COMMENT '',
  `rtn_seg` string COMMENT '',
  `clamshell` string COMMENT '',
  `smt` string COMMENT '',
  `modem_manufacturer` string COMMENT 'Manufacturer of the Modem',
  `model` string COMMENT 'Modem Model Info',
  `firmware` string COMMENT '',
  `bb_tier` string COMMENT 'Broadband Tier',
  `hh_type` string COMMENT 'Household Type',
  `contract_type` string COMMENT '',
  `address` string COMMENT 'Address of the Subscriber',
  `time_zone` string COMMENT 'Standard timezone designation. ** EST+1 refers to Atlantic Standard Time',
  `is_dst` string COMMENT 'Y - If zipcode participates in Daylight Saving Time. Else - N',
  `equipment_status_code` string COMMENT '',
  `billing_src_mac_id` string COMMENT 'MAC address of the subscriber in the format of billing system.',
  `postal_zip_code` string COMMENT '6 character zipcode associated with the Subscriber Address',

  `cmts_cm_us_rx_power_sum` int,
  `cmts_cm_us_signal_noise_sum` int,
  `cmts_cm_us_rx_power_max` int,
  `cmts_cm_us_signal_noise_max` int,
  `cmts_cm_us_rx_power_avg` double,
  `cmts_cm_us_signal_noise_avg` double,
  `cmts_cm_us_uncorrectables` int,
  `cmts_cm_us_correcteds` int,
  `cmts_cm_us_unerroreds` int,
  `codeword_error_rate` double COMMENT 'sum OF CER for the ENTIRE DATE',
  `hdp_insert_ts` timestamp COMMENT 'Hadoop Insert Timestamp',
  `hdp_update_ts` timestamp COMMENT 'Hadoop Update Timestamp')
PARTITIONED BY (
  `year` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES (
  'transient_lastDdlTime'='1572377802');
