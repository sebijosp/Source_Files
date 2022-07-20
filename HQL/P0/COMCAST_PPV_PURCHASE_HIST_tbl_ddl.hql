create table iptv.comcast_ppv_purchase_hist(
    purchase_token varchar(100),
    billing_account_id varchar(100),
    x1_account_id varchar(100),
    device_id varchar(100),
    x1_device_id varchar(100),
    purchase_time timestamp,
    transaction_amount decimal(10,2),
    purchase_type varchar(50),
    billing_status varchar(50),
    offer_id varchar(100),
    media_guids varchar(1000),
    is_preorder boolean,
    offer_category varchar(50),
    hdp_create_ts   timestamp,
    hdp_update_ts   timestamp
   )
   partitioned by (purchase_date date)
   stored as orc
