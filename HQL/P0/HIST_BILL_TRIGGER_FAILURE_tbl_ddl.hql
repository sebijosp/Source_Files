CREATE EXTERNAL TABLE iptv.hist_bill_trigger_failure (
    mac_address                 varchar(255),
    provisioned_date             string,
    Discovery_date               string,
    Last_triggered_date          string,
    hdp_file_name                string,
    hdp_create_ts                timestamp,
    hdp_update_ts                timestamp
    )
  PARTITIONED BY (received_date Date)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  WITH SERDEPROPERTIES (
        "separatorChar" = "|,",
        "quoteChar"     = "\""
         )
  STORED AS TEXTFile
  LOCATION '/apps/hive/warehouse/iptv.db/hist_bill_trigger_failure'
