create or replace view data_pods.VW_WL_GA360_HUP_SUB_HRLY as 
select 
hup_seq_no as Transaction_ID,
DEALER_TYPE as Transaction_Affiliation,
CASE WHEN (MSF + MONTHLY_INSTALLMENT_AMT) =0  THEN HARDWARE_CHG ELSE (MSF + MONTHLY_INSTALLMENT_AMT) END  as Transaction_Revenue,
CASE WHEN acc_ahup_balance=0 THEN(TAX_GST_AMT+TAX_HST_AMT+TAX_PST_AMT+TAX_QST_AMT) ELSE acc_ahup_balance END  as Transaction_Tax,
DEVICE_MODEL as Product_Name,
msrp_price as Product_Price,
initcap(manf_code) as Product_Brand,
initcap(manf_code) as Device_Manufacturer,
swap_req_imei_model as Product_SKU,
'device' as Product_Category,
lower(colour_code) as Product_Variant,
cast(1 as smallint) as Product_Quantity,
'CAD' as Currency_Code,
PROGRAM_CODE as Coupon_Code,
'purchase' as Product_Action,
INIT_PP as Price_Plan,
device_class_desc as Device_Type,
device_model as Device_Model,
price_plan_type_4 as Tier,
'HUP' as Transaction_Type,
sys_creation_date as Transaction_Timestamp,
dlr_name as Store_Location_Name,
adr_state_code as Store_Location_Region,
adr_city as Store_Location_City,
if (postal_code IS NOT NULL and trim(postal_code) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(postal_code) AS STRING))) AS VARCHAR(64)),'') AS Customer_Postal_Code,
if (e_mail IS NOT NULL and  trim(e_mail) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(e_mail) AS STRING))) AS VARCHAR(64)),'') AS customer_email,
if (e_mail IS NOT NULL and  trim(e_mail) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(e_mail) AS STRING))) AS VARCHAR(64)),'') AS Login_ID,
segment_desc as Customer_Segment,
cast(1 as string) as Protocol_Version,
CASE WHEN brand = 'ROGERS' THEN 'UA-5986965-25' WHEN brand='FIDO' THEN 'UA-5986965-26' ELSE 'NA' END AS Tracking_ID,
'event' as Hit_type,
'Offline Sale' as Data_Source,
CASE WHEN brand = 'ROGERS' THEN 'https://www.rogers.com' WHEN brand='FIDO' THEN 'https://www.fido.ca' ELSE 'NA' END AS Document_Host_Name,
dlr_name as Campaign_Source,
'Offline Sale' as Campaign_Medium,
if(ban IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(ban AS STRING))) AS VARCHAR(64)),NULL) as USER_ID,
segment_desc as User_type,
brand,
run_date
from data_pods.WL_HUP_SUB_EVENT_HRLY ;
