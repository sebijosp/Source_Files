CREATE EXTERNAL TABLE IF NOT EXISTS simplifi.campaign_event_detail_fct_external(
--serial_no BIGINT,
time_event_seconds_time        STRING,
details_sifi_uid        STRING,
details_ip        STRING,
details_transaction_id        STRING,
companies_platform_id        INT,
companies_company_id        INT,
companies_client_id        INT,
campaign_campaign_id        INT,
ad_ad_id        INT,
ad_ad_size        STRING,
domain_domain_name        STRING,
location_country        STRING,
location_region        STRING,
location_metro        STRING,
location_city        STRING,
location_postal_code        STRING,
browser_browser_name        STRING,
browser_os_name        STRING,
segment_details_segment_name        STRING,
browser_device_type        STRING,
browser_inventory_type        STRING,
browser_is_app        STRING,
browser_is_ctv        STRING,
browser_is_mobile        STRING,
context_context_category        STRING,
context_context_name        STRING,
performance_clicks        INT,
performance_impressions        INT,
conversions_viewthrough_total        INT,
conversions_clickthrough_total        INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
LOCATION '${hiveconf:filePath}'
TBLPROPERTIES ("skip.header.line.count"="1");
