DROP TABLE data_pods.POD_RETAILSSP_CTN_DLY;
CREATE TABLE data_pods.POD_RETAILSSP_CTN_DLY(
  ban varchar(12),
  ctn varchar(50),
  trans_type string,
  trans_event varchar(50),
  trans_status varchar(20),
  trans_date date,
  trans_time timestamp,
  transaction_direction string,
  dealer_code varchar(5),
  transaction_id varchar(10),
  grouped_transactions string,
  store_code varchar(10),
  chain_code varchar(10),
  brsc_id decimal(18,0),
  franchise varchar(1),
  location_name varchar(140),
  chain_name varchar(140),
  region varchar(50),
  city varchar(100),
  province varchar(12),
  channel_type_desc varchar(60),
  location_type varchar(510))
PARTITIONED BY (
  year int,
  month int,
  day int);
