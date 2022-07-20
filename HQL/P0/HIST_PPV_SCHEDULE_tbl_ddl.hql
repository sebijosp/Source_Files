CREATE EXTERNAL TABLE iptv.hist_ppv_schedule (
    vsp_id  decimal(38,0),
    status  varchar(100),
    station_id      varchar(100),
    program_id      varchar(100),
    title   varchar(255),
    rating  varchar(30),
    genre   varchar(100),
    offer_id        varchar(100),
    bill_text       varchar(255),
    gracenote_id    varchar(100),
    impulse varchar(10),
    price_pennies   decimal(10),
    program_air_time_utc    date,
    duration_min    int,
    buy_window_start_utc    date,
    buy_window_end_utc      date,
    lease_duration_min      int,
    raw_data        varchar(255),
    created_date    timestamp,
    updated_date    timestamp,
    media_guid      varchar(255),
    purchase_count  int,
    cancellation_count     int,
    revenue decimal(10,2),
    origin  varchar(255),
    format  varchar(255),
    studio_id       varchar(255),
    studio_name     varchar(255),
    studio_royalty_pct      varchar(255),
    split_cad       varchar(255),
    split_usd       varchar(255),
    marketing_recovery_pct  varchar(255),
    copyright_recovery_pct  varchar(255),
    offer_language  varchar(255),
    asset_type      varchar(255),
    event_type      varchar(255),
    promotion_category      varchar(255),
    promotion_pct   varchar(255),
    discount_category       varchar(255),
    discount_pct    varchar(255),
    hdp_file_name   varchar(255),
    hdp_create_ts   timestamp,
    hdp_update_ts   timestamp
    )
  PARTITIONED BY (received_date Date)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  WITH SERDEPROPERTIES (
        "separatorChar" = "\,",
        "quoteChar"     = "\""
    )
  STORED AS TEXTFile
  LOCATION '/apps/hive/warehouse/iptv.db/hist_ppv_schedule' 
