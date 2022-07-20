CREATE or replace VIEW data_pods.VW_WL_GA360_R4B_PPC_SUB_HRLY AS
select
    trx_seq_no as Transaction_ID,
    dealer_type as Transaction_Affiliation,
    soc_pp_rate as Transaction_Revenue,
    0 as Transaction_Tax,
    soc_description as Product_Name,
    soc_pp_rate as Product_Price,
    cast(NULL as string) as Product_Brand,
    soc as Product_SKU,
    'plan' as Product_Category,
    cast(NULL as string) as Product_Variant,
    cast(1 as smallint) as Product_Quantity,
    'purchase' as Product_Action,
    soc as Price_Plan,
    cast(NULL as varchar(50)) as Device_Type,
    cast(NULL as string) as Device_Model,
    'PPC' as Transaction_Type,
    event_dttm as Transaction_Timestamp,
    dlr_name as Store_Location_Name,
    dlr_adr_state_code as Store_Location_Region,
    dlr_adr_city as Store_Location_City,
    if (DLR_ADR_ZIP IS NOT NULL and trim(DLR_ADR_ZIP) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(DLR_ADR_ZIP) AS STRING))) AS VARCHAR(64)),'') AS Customer_Postal_Code,
    if (e_mail IS NOT NULL and trim(e_mail) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(e_mail) AS STRING))) AS VARCHAR(64)),'') AS customer_email,
    if (e_mail IS NOT NULL and trim(e_mail) !='',cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM (cast (lower(e_mail) AS STRING))) AS VARCHAR(64)),'') AS Login_ID,
    segment_desc as Customer_Segment,
    cast(1 as string) as Protocol_Version,
    'UA-5986965-27' AS Tracking_ID,
    'event' as Hit_type,
    'Offline Sale' as Data_Source,
    CASE WHEN brand = 'ROGERS' THEN 'https://www.rogers.com' WHEN brand='FIDO' THEN 'https://www.fido.ca' ELSE 'NA' END AS Document_Host_Name,
    if(pe.ban IS NOT NULL, cast(reflect('org.apache.commons.codec.digest.DigestUtils','sha256Hex',TRIM(cast(pe.ban AS STRING))) AS VARCHAR(64)),NULL) as USER_ID,
    'CAD' as Currency_Code,
    cast(NULL as char(12)) as Coupon_Code,
    segment_desc as User_type,    
    brand,
    run_date
from
    data_pods.WL_PPC_SUB_EVENT_HRLY pe
where 1=1
    AND PPC_STATUS = 'PPC effective immediately'
;
