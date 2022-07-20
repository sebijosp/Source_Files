create or replace view data_pods.VW_META_CH_NAC_SUB_EVENT_HRLY as
select
ap_id as order_id,
cable_channel_level_1 as data_source_1,
cable_channel_level_3 as data_source_2,
if (e_mail IS NOT NULL and  trim(e_mail) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(e_mail) AS STRING))) AS VARCHAR(64)),'') AS email,
'' AS phone,
if (customer_postal_code IS NOT NULL and trim(customer_postal_code) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(customer_postal_code) AS STRING))) AS VARCHAR(64)),'') AS zip,
if (ban IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(ban AS STRING))) AS VARCHAR(64)),NULL) as extern_id,
if (customer_first_name IS NOT NULL and  trim(customer_first_name) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(customer_first_name) AS STRING))) AS VARCHAR(64)),'') AS fn,
if (customer_last_name IS NOT NULL and  trim(customer_last_name) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(customer_last_name) AS STRING))) AS VARCHAR(64)),'') AS ln,
if (customer_dob IS NOT NULL and  customer_dob !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (customer_dob AS STRING))) AS VARCHAR(64)),'') AS dob,
if (customer_city IS NOT NULL and  trim(customer_city) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(customer_city) AS STRING))) AS VARCHAR(64)),'') AS ct,
if (customer_state IS NOT NULL and  trim(customer_state) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(customer_state) AS STRING))) AS VARCHAR(64)),'') AS st,
if (customer_country IS NOT NULL and  trim(customer_country) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(customer_country) AS STRING))) AS VARCHAR(64)),'') AS country,
transaction_timestamp as event_time,
'purchase' as event_name,
'CAD' as currency,
transaction_revenue as value,
tier as name,
transaction_revenue as price,
'NA' as brand,
'NA' as device_manufacturer,
service_type as category,
'NA' as sku,
'NA' as variant,
cast(1 as smallint) as quantity,
charge_code as coupon_code,
prod_ref_id as price_plan,
'NA' as device_type,
'NA' as device_model,
tier as tier_plan,
'NAC' as transaction_type,
store_location_name,
store_location_region,
store_location_city,
'NA' as customer_segment,
customer_type as rogers_fido,
run_date
from data_pods.CH_NAC_SUB_EVENT_HRLY;
