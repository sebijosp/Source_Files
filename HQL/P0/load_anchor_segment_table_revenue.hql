set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


With Pre_MAESTRO_REV_ECID as
(
SELECT
    from_unixtime(unix_timestamp(activity_date ,"YYYY-MM-DD"), "YYYYMM") PERIOD,
    ENTERPRISE_ID,
    PRODUCT_BRAND,
    PRODUCT_LOB,
    CASE
    WHEN (PRODUCT_LOB = 100 and product_brand = 'Rogers') THEN 'ROG_IG_INTERNET'
    WHEN  PRODUCT_LOB = 200 THEN 'IPTV'
    WHEN  PRODUCT_LOB = 600 THEN 'RHP'
    END AS LOB,
    rank () OVER (PARTITION BY ENTERPRISE_ID, PRODUCT_LOB,PRODUCT_BRAND ORDER BY ACTIVITY_DATE DESC) AS RNK,
    PRODUCT_MSF_NET ,
    PRODUCT_DISCOUNT ,
    ACTIVITY_GRADE_RATE ,
    CONSOLIDATED_IND
FROM
    APP_IBRO.VW_IBRO_SUBSCRIBER_CLOSING_HIST
WHERE
    (ACTIVITY_DATE >= '${hiveconf:LAST_MONTH_START_IN}' and ACTIVITY_DATE <= '${hiveconf:LAST_MONTH_END_IN}')
    AND PRODUCT_LOB_UNIT = 'Y'
    AND ACTIVITY in ('CL')
    AND PRODUCT_LOB_UNIT in ('Y', 'F' )
    AND PRODUCT_SOURCE IN ('MS')
    AND PRODUCT_BRAND IN ('Rogers', 'Fido')
    AND PRODUCT_LOB IN ('100','200','600')
    AND product_segment = 'CBU'
    AND Product_Source = 'MS'
),

MAESTRO_REV_ECID as
(
SELECT
    ENTERPRISE_ID,
    PRODUCT_BRAND,
    LOB,
    SUM (PRODUCT_MSF_NET) AS REVENUE,
    SUM (PRODUCT_DISCOUNT) as DISCOUNT,
    ACTIVITY_GRADE_RATE,
    CONSOLIDATED_IND,
    PERIOD
FROM  Pre_MAESTRO_REV_ECID
WHERE RNK = 1
GROUP BY ENTERPRISE_ID, PRODUCT_BRAND, LOB, ACTIVITY_GRADE_RATE, CONSOLIDATED_IND, PERIOD
),

MAESTRO_REV1_ECID as
(
SELECT
       PERIOD,
       ENTERPRISE_ID,
       ACTIVITY_GRADE_RATE,
       CONSOLIDATED_IND,
       PRODUCT_BRAND,
       LOB as PRODUCT_LOB,
       sum(CASE WHEN LOB='IPTV' THEN 1 ELSE 0 END) AS IPTV,
       sum(CASE WHEN LOB='ROG_IG_INTERNET' THEN 1 ELSE 0 END) AS ROG_IG_INTERNET,
       sum(CASE WHEN LOB='RHP' THEN 1 ELSE 0 END) AS RHP,
       sum(case when LOB='IPTV'   then  REVENUE else 0 end) as REVENUE_IPTV,
       sum(case when LOB='ROG_IG_INTERNET'  then  REVENUE else 0 end) as REVENUE_ROG_IG_INT,
       sum(case when LOB='RHP'  then  REVENUE else 0 end) as REVENUE_RHP,
       sum(case when LOB='IPTV' then  DISCOUNT else 0 end) as DISCOUNT_IPTV,
       sum(case when LOB='ROG_IG_INTERNET' then  DISCOUNT else 0 end) as DISCOUNT_ROG_IG_INT,
       sum(case when LOB='RHP' then  DISCOUNT else 0 end) as DISCOUNT_RHP
FROM
       Maestro_rev_ECID
Group by
       PERIOD,
       ENTERPRISE_ID,
       ACTIVITY_GRADE_RATE,
       CONSOLIDATED_IND,
       PRODUCT_BRAND,
       LOB
order by
       ENTERPRISE_ID
),

Pre_IBRO_PRODUCT_DATE_ECID as
(
SELECT  distinct
        calendar_period as period,
        ec_id as enterprise_id,
        prod_ref_id,
        prod_start_bill_date as product_start_date,
        prod_stop_bill_date as product_end_date
FROM
        APP_MAESTRO.PNTRDLY
WHERE
        prod_ref_id in ('*HMSC', '*INFC', 'PBA1','PSFF','PMFF','PVFF','PLFF', '*INRC', '*RHPC' )
) ,

IBRO_PRODUCT_DATE_ECID as
(
SELECT
            enterprise_id,
            min(case when  prod_ref_id in ('PBA1','PSFF','PMFF','PVFF','PLFF')  then  PRODUCT_START_DATE else null end) as IPTV_START_DATE,
            max(case when  prod_ref_id in ('PBA1','PSFF','PMFF','PVFF','PLFF')  then  PRODUCT_END_DATE else null end) as IPTV_END_DATE,
            min(case when prod_ref_id in ('*INRC') then PRODUCT_START_DATE else null end) as INT_start_date,
            max(case when prod_ref_id in ('*INRC') then PRODUCT_END_DATE else null end) as INT_END_date,
            min(case when prod_ref_id in ('*RHPC') then PRODUCT_START_DATE else null end) as RHP_start_date,
            max(case when prod_ref_id in ('*RHPC') then PRODUCT_END_DATE else null end) as RHP_END_date
FROM
        Pre_IBRO_PRODUCT_DATE_ECID
group by
        enterprise_id
),

MAESTRO_TENURE1_ECID as
(
select
      enterprise_id ,
      to_date(IPTV_START_DATE) as IPTV_start_date,
      to_date(INT_START_DATE) as INT_start_date,
      to_date(RHP_START_DATE) as RHP_start_date,
          case when to_date(IPTV_END_DATE) > '${hiveconf:LAST_MONTH_END_IN}'  then '${hiveconf:LAST_MONTH_END_IN}' else to_date(IPTV_END_DATE) END AS IPTV_end_date,
      case when to_date(INT_END_DATE) > '${hiveconf:LAST_MONTH_END_IN}'  then '${hiveconf:LAST_MONTH_END_IN}' else to_date(INT_END_DATE) END AS INT_end_date,
      case when to_date(RHP_END_DATE) > '${hiveconf:LAST_MONTH_END_IN}'  then '${hiveconf:LAST_MONTH_END_IN}' else to_date(RHP_END_DATE) END AS RHP_end_date
from
      ibro_product_date_ECID
),

MAESTRO_TENURE_ECID as
(
SELECT
      enterprise_id,
      CASE WHEN Iptv_START_DATE is not NULL and Iptv_START_DATE <> ' '  THEN cast(datediff(Iptv_end_date , Iptv_START_DATE)/30 as int) ELSE null  END AS TENURE_MONTH_Iptv ,
      CASE WHEN INT_START_DATE is not NULL and INT_START_DATE <> ' '  THEN cast(datediff(INT_end_date , INT_START_DATE)/30 as int) ELSE null  END AS TENURE_MONTH_INT,
      CASE WHEN RHP_START_DATE is not NULL and RHP_START_DATE <> ' '  THEN cast(datediff(RHP_end_date , RHP_START_DATE)/30 as int) ELSE null  END AS TENURE_MONTH_RHP
FROM
      maestro_tenure1_ECID
Order by
      enterprise_id
),

Pre_MAESTRO_AGE_ECID as
(
SELECT
              ROW_NUMBER() OVER (PARTITION BY X_RCIS_ID ORDER BY Update_Stamp DESC) AS Rnk,
              X_RCIS_ID,
              TO_DATE(SUBSTR(BIRTH_DATE, 1, 10)) AS PROFILE_BIRTH_DATE,
              Update_Stamp
        FROM
              ELA_CRM.table_contact
),

MAESTRO_AGE_ECID as
(
SELECT
        X_RCIS_ID,
        PROFILE_BIRTH_DATE,
        CAST(datediff(current_date() , PROFILE_BIRTH_DATE )/365 as int)    AS AGE
FROM
        Pre_MAESTRO_AGE_ECID
WHERE
       Rnk = 1
),

Pre_MAESTRO_REV_AGE_ECID AS
(
SELECT
        a.*, b.age
FROM
        Maestro_rev1_ECID a
left join
        maestro_age_ECID b
ON      a.enterprise_id = cast(b.x_rcis_id as decimal(9,0))
),

MAESTRO_REV_AGE_ECID AS
(
SELECT
       distinct
       PERIOD,
       ENTERPRISE_ID,
       ACTIVITY_GRADE_RATE,
       CONSOLIDATED_IND,
       PRODUCT_BRAND,
       PRODUCT_LOB,
       IPTV,
       ROG_IG_INTERNET,
       RHP,
       REVENUE_IPTV,
       REVENUE_ROG_IG_INT,
       REVENUE_RHP,
       DISCOUNT_IPTV,
       DISCOUNT_ROG_IG_INT,
       DISCOUNT_RHP,
       Age
FROM
       Pre_MAESTRO_REV_AGE_ECID
),

Pre_ABC_ECID as
(
Select
         ec_id, max(a.PROD_START_BILL_DATE) as PROD_START_BILL_DATE
from
         APP_MAESTRO.PNTRDLY a
left join
         app_maestro.addrdim b
on       a.ADDRESS_SEQ = b.ADDRESS_KEY
where
         from_unixtime(unix_timestamp(a.CALENDAR_DATE ,"YYYY-MM-DD"), "YYYYMM") = '${hiveconf:PROCESS_MONTH_IN}'
         and b.CRNT_F = 'Y'
group by
         ec_id
),

ABC_ECID as
(
Select
         distinct ec_id, PROD_START_BILL_DATE
from
         Pre_ABC_ECID
),

CDE_ECID as
(
select
        ec_id, srvc_pstl_cd, PROD_START_BILL_DATE
from
        MAESTRO.PNTRDLY
group by
        ec_id, srvc_pstl_cd, PROD_START_BILL_DATE
),

MAESTRO_PD2_ECID as
(
select
        distinct x.ec_id, x.SRVC_PSTL_CD
from
        cde_ECID x
inner join
        abc_ECID z
on
        x.ec_id = z.ec_id
        and
        x.PROD_START_BILL_DATE = z.PROD_START_BILL_DATE
),

MAESTRO_PD_ECID as
(
select
        ec_id,
        max(SRVC_PSTL_CD) as SRVC_PSTL_CD
from    maestro_pd2_ECID
group by
        ec_id
),

INCABLE_PD_ECID as
(
SELECT
            postal_zip_code,
            SUBSTR(postal_zip_code,1,3) AS FSA,
            MAX(CASE WHEN postal_zip_code IS NOT NULL
              AND
              LENGTH(TRIM(postal_zip_code)) = 6
              AND
              (ADDR_IN_TERR_IND <> 'N' OR ADDR_IN_TERR_IND IS NULL)
              AND
              SERVICABILITY_CODE NOT IN ('T', 'D')
              AND
              company_number < '270'
              AND
              dummy_addr_ind <> 'Y' THEN 1 ELSE 0 END) AS INFOOTPRINT_POSTAL_CODE
FROM
            marquee.address  A
GROUP BY
            postal_zip_code,
            SUBSTR(postal_zip_code,1,3)
),

INCABLE_FSA_ECID as
(
SELECT
           FSA, MAX(CASE WHEN SUBSTR(FSA, 2, 1) <> '0' THEN INFOOTPRINT_POSTAL_CODE ELSE 0 END) AS INFOOTPRINT_FSA
FROM
           incable_pd_ECID
GROUP BY
           FSA
order by
           FSA
),

FOOTPRINT_ECID as
(
select    distinct  ec_id, SRVC_PSTL_CD,
          case when b.postal_zip_code is not null and b.INFOOTPRINT_POSTAL_CODE = 1 then 1
                when b.postal_zip_code is not null and b.INFOOTPRINT_POSTAL_CODE = 0 then 0
                when b.postal_zip_code is null and  c.fsa is not null and c.INFOOTPRINT_fsa = 1 then 1 else 0 end as flg_in_cablefootprint,
          CASE WHEN COALESCE(b.INFOOTPRINT_POSTAL_CODE, 0) = 1 OR COALESCE(c.INFOOTPRINT_fsa, 0) = 1 THEN 1 ELSE 0 END AS FLG_IN_CABLEFP_ucar
FROM
           maestro_pd_ECID a
left join
           incable_pd_ECID b
on         a.SRVC_PSTL_CD = b.postal_zip_code
left join
           incable_fsa_ECID c
on         substr(SRVC_PSTL_CD, 1, 3) = c.fsa
where
           SRVC_PSTL_CD is not null
),

pre_maestro_footprint_ECID as
(
select
           a.*, b.SRVC_PSTL_CD as postal_code, b.FLG_IN_CABLEFOOTPRINT
from
            maestro_rev_age_ECID a
left join
            footprint_ECID b
on
           a.enterprise_id = b.ec_id
order by
           enterprise_id
),

maestro_footprint_ECID AS
(
SELECT
       distinct
       PERIOD,
       ENTERPRISE_ID,
       ACTIVITY_GRADE_RATE,
       CONSOLIDATED_IND,
       PRODUCT_BRAND,
       PRODUCT_LOB,
       IPTV,
       ROG_IG_INTERNET,
       RHP,
       REVENUE_IPTV,
       REVENUE_ROG_IG_INT,
       REVENUE_RHP,
       DISCOUNT_IPTV,
       DISCOUNT_ROG_IG_INT,
       DISCOUNT_RHP,
       Age,
       postal_code,
       FLG_IN_CABLEFOOTPRINT
FROM
       pre_maestro_footprint_ECID
),

HalfYear_Average_ECID as
(
select
    ENTERPRISE_ID,
    PRODUCT_BRAND ,
    PRODUCT_LOB ,
    CASE
    WHEN (PRODUCT_LOB = 100 and product_brand = 'Rogers') THEN 'ROG_IG_INTERNET'
    WHEN  PRODUCT_LOB = 200 THEN 'IPTV'
    WHEN  PRODUCT_LOB = 600 THEN 'RHP'
    END AS LOB,
    case when PRODUCT_LOB='100' then  avg(PRODUCT_MSF_NET) else 0 end as REVENUE_HalfYear_Internet,
    case when PRODUCT_LOB = '200'
         then  avg(PRODUCT_MSF_NET) else 0 end as REVENUE_HalfYear_IPTV,
    case when PRODUCT_LOB='600' then  avg(PRODUCT_MSF_NET) else 0 end as REVENUE_HalfYear_RHP,
    ACTIVITY_GRADE_RATE ,
    CONSOLIDATED_IND
From
   APP_IBRO.VW_IBRO_SUBSCRIBER_CLOSING_HIST
WHERE
    (ACTIVITY_DATE >= '${hiveconf:LAST_6_MONTH_START_IN}' and ACTIVITY_DATE <= '${hiveconf:LAST_6_MONTH_END_IN}')
    AND PRODUCT_LOB_UNIT = 'Y'
    AND ACTIVITY in ('CL')
    AND PRODUCT_LOB_UNIT in ('Y', 'F' )
    AND PRODUCT_SOURCE IN ('MS')
    AND PRODUCT_BRAND IN ('Rogers')
    AND PRODUCT_LOB IN ('100' , '200' , '600')
    AND product_segment = 'CBU'
    AND Product_Source = 'MS'
GROUP BY ENTERPRISE_ID, PRODUCT_BRAND, PRODUCT_LOB,  product_code ,ACTIVITY_GRADE_RATE, CONSOLIDATED_IND
),

Prv_HalfYear_Average_ECID as
(
select
    ENTERPRISE_ID,
    PRODUCT_BRAND ,
    PRODUCT_LOB ,
    CASE
    WHEN (PRODUCT_LOB = 100 and product_brand = 'Rogers') THEN 'ROG_IG_INTERNET'
    WHEN  PRODUCT_LOB = 200 THEN 'IPTV'
    WHEN  PRODUCT_LOB = 600 THEN 'RHP'
    END AS LOB,
    case when PRODUCT_LOB='100' then  avg(PRODUCT_MSF_NET) else 0 end as REVENUE_Prv_HalfYear_Internet,
    case when PRODUCT_LOB ='200'
         then  avg(PRODUCT_MSF_NET) else 0 end as REVENUE_Prv_HalfYear_IPTV,
    case when PRODUCT_LOB='600' then  avg(PRODUCT_MSF_NET) else 0 end as REVENUE_Prv_HalfYear_RHP,
    ACTIVITY_GRADE_RATE ,
    CONSOLIDATED_IND
From
   APP_IBRO.VW_IBRO_SUBSCRIBER_CLOSING_HIST
WHERE
    ACTIVITY_DATE >= '${hiveconf:LAST_Prv_6_MONTH_START_IN}' and ACTIVITY_DATE <= '${hiveconf:LAST_Prv_6_MONTH_END_IN}'
    AND PRODUCT_LOB_UNIT = 'Y'
    AND ACTIVITY in ('CL')
    AND PRODUCT_LOB_UNIT in ('Y', 'F' )
    AND PRODUCT_SOURCE IN ('MS')
    AND PRODUCT_BRAND IN ('Rogers')
    AND PRODUCT_LOB IN ('100' , '200' , '600')
    AND product_segment = 'CBU'
    AND Product_Source = 'MS'
GROUP BY ENTERPRISE_ID, PRODUCT_BRAND, PRODUCT_LOB, product_code,  ACTIVITY_GRADE_RATE, CONSOLIDATED_IND
),

Pre_Growth_Average_ECID as
(
select
    a.ENTERPRISE_ID,
    a.PRODUCT_BRAND ,
    a.PRODUCT_LOB ,
    a.LOB,
    case when a.REVENUE_HalfYear_Internet > 0 and a.PRODUCT_LOB='100'
                   then  (a.REVENUE_HalfYear_Internet/b.REVENUE_Prv_HalfYear_Internet) - 1 else 0 end as INTERNET_REV_GR_IG ,
    case when a.REVENUE_HalfYear_IPTV > 0  and a.PRODUCT_LOB ='200'
                   then  (a.REVENUE_HalfYear_IPTV/b.REVENUE_Prv_HalfYear_IPTV) - 1 else 0 end as TV_REV_GR_IG  ,
    case when a.REVENUE_HalfYear_RHP > 0 and a.PRODUCT_LOB='600'
                   then  (a.REVENUE_HalfYear_RHP/b.REVENUE_Prv_HalfYear_RHP) - 1 else 0 end as RHP_REV_GR_IG   ,
    a.ACTIVITY_GRADE_RATE ,
    a.CONSOLIDATED_IND
from HalfYear_Average_ECID a
left join Prv_HalfYear_Average_ECID b
on a.ENTERPRISE_ID = b.ENTERPRISE_ID  and
   a.PRODUCT_BRAND = b.PRODUCT_BRAND  and
   a.PRODUCT_LOB = b.PRODUCT_LOB and
   a.ACTIVITY_GRADE_RATE = b.ACTIVITY_GRADE_RATE and
   a.CONSOLIDATED_IND = b.CONSOLIDATED_IND
),

Growth_Average_ECID as
(
select
    ENTERPRISE_ID,
    PRODUCT_BRAND ,
    LOB ,
    SUM(INTERNET_REV_GR_IG) as INTERNET_REV_GR_IG,
    SUM(TV_REV_GR_IG) as TV_REV_GR_IG,
    SUM(RHP_REV_GR_IG) as RHP_REV_GR_IG,
    ACTIVITY_GRADE_RATE    ,
    CONSOLIDATED_IND
from
    Pre_Growth_Average_ECID
Group By
    ENTERPRISE_ID,
    PRODUCT_BRAND ,
    LOB ,
    ACTIVITY_GRADE_RATE ,
    CONSOLIDATED_IND
),

Stage1_final_table_ECID as
(
select
     TAB1.ENTERPRISE_ID ,
     TAB1.IPTV  ,
     TAB1.ROG_IG_INTERNET ,
     TAB1.RHP ,
     TAB1.REVENUE_IPTV  ,
     TAB1.REVENUE_ROG_IG_INT ,
     TAB1.REVENUE_RHP ,
     TAB1.DISCOUNT_IPTV ,
     TAB1.DISCOUNT_ROG_IG_INT ,
     TAB1.DISCOUNT_RHP ,
     TAB1.PRODUCT_BRAND,
     TAB1.PRODUCT_LOB,
     TAB1.ACTIVITY_GRADE_RATE    ,
     TAB1.CONSOLIDATED_IND   ,
     TAB2.TENURE_MONTH_Iptv ,
     TAB2.TENURE_MONTH_Int ,
     TAB2.TENURE_MONTH_RHP ,
     TAB3.Age,
     TAB4.postal_code ,
     TAB4.FLG_IN_CABLEFOOTPRINT
from
     MAESTRO_REV1_ECID  TAB1
left join
     MAESTRO_TENURE_ECID  TAB2
                       ON  TAB1.ENTERPRISE_ID  = TAB2.ENTERPRISE_ID
left join
      MAESTRO_REV_AGE_ECID  TAB3
                       ON  TAB1.ENTERPRISE_ID  = TAB3.ENTERPRISE_ID
left join
      maestro_footprint_ECID  TAB4
                       ON  TAB1.ENTERPRISE_ID  = TAB4.ENTERPRISE_ID
),

Staging2_final_table_ECID as
(
select
    distinct
     ENTERPRISE_ID,
     PRODUCT_BRAND,
     PRODUCT_LOB,
     ACTIVITY_GRADE_RATE ,
     CONSOLIDATED_IND   ,
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
     TENURE_MONTH_INT ,
     TENURE_MONTH_RHP ,
     Age,
     postal_code ,
     FLG_IN_CABLEFOOTPRINT
from
  Stage1_final_table_ECID
),

Pre_final_table_ECID as
(
select
     TAB1.ENTERPRISE_ID,
     TAB1.PRODUCT_BRAND,
     TAB1.PRODUCT_LOB,
     TAB1.ACTIVITY_GRADE_RATE ,
     TAB1.CONSOLIDATED_IND   ,
     TAB1.IPTV  ,
     TAB1.ROG_IG_INTERNET ,
     TAB1.RHP ,
     TAB1.REVENUE_IPTV  ,
     TAB1.REVENUE_ROG_IG_INT ,
     TAB1.REVENUE_RHP ,
     TAB1.DISCOUNT_IPTV ,
     TAB1.DISCOUNT_ROG_IG_INT ,
     TAB1.DISCOUNT_RHP ,
     TAB1.TENURE_MONTH_Iptv ,
     TAB1.TENURE_MONTH_INT ,
     TAB1.TENURE_MONTH_RHP ,
     TAB1.Age,
     substr(TAB1.postal_code, 1, 3)  as Postal_Code,
     TAB1.FLG_IN_CABLEFOOTPRINT,
     CASE WHEN TAB1.FLG_IN_CABLEFOOTPRINT = 1 then 0
          else 1
     END as FLG_OUT_CABLEFOOTPRINT,
     TAB2.INTERNET_REV_GR_IG,
     TAB2.TV_REV_GR_IG ,
     TAB2.RHP_REV_GR_IG
From
     Staging2_final_table_ECID TAB1
Left join
     Growth_Average_ECID  TAB2
     ON
     TAB1.ENTERPRISE_ID = TAB2.ENTERPRISE_ID  and
     TAB1.PRODUCT_BRAND = TAB2.PRODUCT_BRAND  and
     TAB1.PRODUCT_LOB = TAB2.LOB and
     TAB1.ACTIVITY_GRADE_RATE = TAB2.ACTIVITY_GRADE_RATE and
     TAB1.CONSOLIDATED_IND = TAB2.CONSOLIDATED_IND
),

final_table_ECID as
(
select
      distinct
        ENTERPRISE_ID,
        PRODUCT_BRAND,
        PRODUCT_LOB,
        ACTIVITY_GRADE_RATE ,
        CONSOLIDATED_IND   ,
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
        TENURE_MONTH_INT ,
        TENURE_MONTH_RHP ,
        Age,
        postal_code ,
        FLG_IN_CABLEFOOTPRINT,
        FLG_OUT_CABLEFOOTPRINT,
        INTERNET_REV_GR_IG,
        TV_REV_GR_IG ,
        RHP_REV_GR_IG
from
    Pre_final_table_ECID
),

final_table_ECID_NULL as
(
select
      distinct
        ENTERPRISE_ID,
        PRODUCT_BRAND,
        PRODUCT_LOB,
        ACTIVITY_GRADE_RATE ,
        CONSOLIDATED_IND   ,
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
        TENURE_MONTH_INT ,
        TENURE_MONTH_RHP ,
        Age,
        postal_code ,
        FLG_IN_CABLEFOOTPRINT,
        FLG_OUT_CABLEFOOTPRINT,
        INTERNET_REV_GR_IG,
        TV_REV_GR_IG ,
        RHP_REV_GR_IG
from
    Pre_final_table_ECID
where (PRODUCT_LOB is not null)
),

final_table_ECID_COMBINE as
(
select
        ENTERPRISE_ID,
        Max(CONSOLIDATED_IND) as CONSOLIDATED_IND   ,
        Max(IPTV)  as  IPTV,
        Max(ROG_IG_INTERNET) as ROG_IG_INTERNET,
        Max(RHP) as RHP ,
        SUM(REVENUE_IPTV) as REVENUE_IPTV  ,
        SUM(REVENUE_ROG_IG_INT) as REVENUE_ROG_IG_INT,
        SUM(REVENUE_RHP) as REVENUE_RHP ,
        SUM(DISCOUNT_IPTV) as DISCOUNT_IPTV ,
        SUM(DISCOUNT_ROG_IG_INT) as DISCOUNT_ROG_IG_INT ,
        SUM(DISCOUNT_RHP)  as DISCOUNT_RHP,
        Max(TENURE_MONTH_Iptv) as TENURE_MONTH_Iptv ,
        Max(TENURE_MONTH_INT) as TENURE_MONTH_INT ,
        Max(TENURE_MONTH_RHP) as TENURE_MONTH_RHP ,
        Max(Age) as Age,
        Max(postal_code) as postal_code,
        Max(FLG_IN_CABLEFOOTPRINT) as FLG_IN_CABLEFOOTPRINT,
        Max(FLG_OUT_CABLEFOOTPRINT) as FLG_OUT_CABLEFOOTPRINT ,
        SUM(INTERNET_REV_GR_IG) as INTERNET_REV_GR_IG ,
        SUM(TV_REV_GR_IG) as TV_REV_GR_IG ,
        SUM(RHP_REV_GR_IG) as RHP_REV_GR_IG
from
    final_table_ECID_NULL
Group By ENTERPRISE_ID
)

insert OVERWRITE TABLE ANCHOR_SEGMENT.ANCHOR_SEGMENT_REVENUE_TEMP
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
     RHP_REV_GR_IG
from final_table_ECID_COMBINE;
