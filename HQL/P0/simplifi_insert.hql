set hive.execution.engine=tez;

!echo "INSERTING INTO TABLE: simplifi.CAMPAIGN_EVENT_DETAIL_FCT from simplifi.campaign_event_detail_fct_external ";


INSERT OVERWRITE TABLE simplifi.CAMPAIGN_EVENT_DETAIL_FCT
PARTITION(filedate) 
SELECT 
time_event_seconds_time,
details_sifi_uid,
details_ip,
details_transaction_id,
companies_platform_id,
companies_company_id,
companies_client_id,
campaign_campaign_id,
ad_ad_id,
ad_ad_size,
domain_domain_name,
location_country,
location_region,
location_metro,
location_city,
location_postal_code,
browser_browser_name,
browser_os_name,
segment_details_segment_name,
browser_device_type,
browser_inventory_type,
browser_is_app,
browser_is_ctv,
browser_is_mobile,
context_context_category,
context_context_name,
performance_clicks,
performance_impressions,
conversions_viewthrough_total,
conversions_clickthrough_total,
current_date() as etl_insrt_dt,
cast('${hiveconf:dt}' as DATE) AS filedate 
FROM simplifi.campaign_event_detail_fct_external;

!echo "INSERTING INTO TABLE: simplifi.CAMPAIGN_EVENT_DETAIL_FCT from simplifi.campaign_event_detail_fct_external completed ";
