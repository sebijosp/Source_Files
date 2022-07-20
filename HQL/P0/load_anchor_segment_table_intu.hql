set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

With Initial_Int_Mly_Usage_Act  as
(
SELECT
    b.EC_ID ,
    substr(a.generated_date,1,6) as generated_month,
    a.up_usage,
    a.down_usage
FROM
    maestro_sf_usage.hsi_cdr a
inner join app_maestro.cust_internet_equip_dim b
    on a.mac_address = b.DATA_MAC_ID
WHERE
    (generated_date >= '${hiveconf:LAST_6_MONTH_START_WODASH_IN}' and generated_date <= '${hiveconf:LAST_6_MONTH_END_WODASH_IN}')
    AND CRNT_FLG = 'Y'
    AND event_category = 1
),

Pre_Int_Mly_Usage_Act  as
(
SELECT
      EC_ID,
      generated_month,
      sum(up_usage) as total_up_usage ,
      sum(down_usage) as total_down_usage
FROM  Initial_Int_Mly_Usage_Act
GROUP BY EC_ID, generated_month
),

Stage_Int_Mly_Usage_Act  as
(
SELECT
      EC_ID,
      avg(total_up_usage) as total_up_usage ,
      avg(total_down_usage) as total_down_usage
FROM
Pre_Int_Mly_Usage_Act
GROUP BY EC_ID
),

Int_Mly_Usage_Act  as
(
SELECT
      EC_ID,
      (total_up_usage + total_down_usage)/1024/1024/1024 as INTERNET_MLY_USAGE_ACTUAL
FROM
      Stage_Int_Mly_Usage_Act
),

TMN_THEMEPACK as
(
select
enterprise_id,
COUNT(CASE WHEN PRODUCT_CODE in ('PCRV', 'PCRF', 'PMO1', 'QCRV', 'PMOV', 'FEAT', 'DSPR', 'DSUP') THEN 1 END) AS TMN,
COUNT(CASE WHEN PRODUCT_CODE in ('QT07','QT09','QT41','QT23','QT33','QT37','QT25','QT28','QT46','QT12','QT18','QT14','QT05','QT26','QT17','QT19',
'QT11','QT20','QT44','QT35','QT40','QT06','QT01','QT43','QT02','QT39','QT16','QT13','QT45','QT27','QT07','QT09','QT41','QT23','QT33','QT37''QT25',
'QT28','QT46','QT12','QT18','QT14','QT05','QT26','QT17','QT19','QT11','QT20','QT44','QT35','QT40','QT06','QT01','QT43','QT02','QT39','QT16','QT13',
'QT45','QT27') THEN 1 END) AS THEMEPACK
from app_ibro.ibro_subscriber_activity a
where activity_date>='${hiveconf:LAST_MONTH_START_IN}'
and activity_date<='${hiveconf:LAST_MONTH_END_IN}'
and activity_type in ('A','E')
and activity='NAC'
and PRODUCT_LOB='200'
and product_source='MS'
group by enterprise_id
),

INT_USAGE_TMN_THEMEPACK as
(
select
       a.ENTERPRISE_ID,
       CONSOLIDATED_IND,
       IPTV,
       ROG_IG_INTERNET,
       RHP ,
       REVENUE_IPTV  ,
       REVENUE_ROG_IG_INT,
       REVENUE_RHP ,
       DISCOUNT_IPTV ,
       DISCOUNT_ROG_IG_INT ,
       DISCOUNT_RHP,
       TENURE_MONTH_Iptv ,
       TENURE_MONTH_INT ,
       TENURE_MONTH_RHP ,
       Age,
       postal_code,
       FLG_IN_CABLEFOOTPRINT,
       FLG_OUT_CABLEFOOTPRINT ,
       INTERNET_REV_GR_IG ,
       TV_REV_GR_IG ,
       RHP_REV_GR_IG,
       case when (INTERNET_MLY_USAGE_ACTUAL <> 0 and INTERNET_MLY_USAGE_ACTUAL is not null)  then  INTERNET_MLY_USAGE_ACTUAL else 0 end as INTERNET_MLY_USAGE_ACTUAL,
       case when (TMN <> 0 and TMN is not null)  then  TMN else 0 end as TMN,
       case when (THEMEPACK <> 0 and THEMEPACK is not null)  then  THEMEPACK else 0 end as THEMEPACK
From
      ANCHOR_SEGMENT.ANCHOR_SEGMENT_REVENUE_TEMP  a
LEFT JOIN
      Int_Mly_Usage_Act b
ON
      a.ENTERPRISE_ID = EC_ID
LEFT JOIN
      TMN_THEMEPACK  c
ON
      a.ENTERPRISE_ID = c.ENTERPRISE_ID
)

insert OVERWRITE TABLE ANCHOR_SEGMENT.INT_MLY_USAGE_TMN_THEMEP_TEMP
select
     ENTERPRISE_ID,
     CONSOLIDATED_IND,
     IPTV  ,
     ROG_IG_INTERNET ,
     RHP ,
     REVENUE_IPTV  ,
     REVENUE_ROG_IG_INT ,
     REVENUE_RHP ,
     DISCOUNT_IPTV ,
     DISCOUNT_ROG_IG_INT ,
     DISCOUNT_RHP ,
     TENURE_MONTH_Iptv ,
     TENURE_MONTH_INT  ,
     TENURE_MONTH_RHP ,
     Age,
     postal_code ,
     FLG_IN_CABLEFOOTPRINT,
     FLG_OUT_CABLEFOOTPRINT ,
     INTERNET_REV_GR_IG,
     TV_REV_GR_IG ,
     RHP_REV_GR_IG,
     INTERNET_MLY_USAGE_ACTUAL,
     TMN,
     THEMEPACK,
     100000 ,
     1
from INT_USAGE_TMN_THEMEPACK;
