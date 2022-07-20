set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

With Stage_RHP_CALLS as
(
select a.*,  b.account  from anchor_segment.INT_MLY_USAGE_TMN_THEMEP_TEMP a left join hash_lookup.ecid_ban_can b on a.enterprise_id = b.ecid
),

Pre_RHP_CALLS as
(
SELECT
      TAB1.*,
      CASE WHEN l9_ld_call_type='01'  and  resource_type='TN' and  TAB2.cycle_code=2 and TAB2.cycle_instance='${hiveconf:PRCOESS_MONTH_CYCLE_IN}' and TAB2.cycle_year='${hiveconf:PRCOESS_YEAR_CYCLE_IN}'  then l3_charge_amount
      else 0
      END as RHP_Local_calls,
      CASE WHEN l9_ld_call_type in('04' , '03') and resource_type='TN' and TAB2.cycle_code=2 and TAB2.cycle_instance='${hiveconf:PRCOESS_MONTH_CYCLE_IN}' and TAB2.cycle_year='${hiveconf:PRCOESS_YEAR_CYCLE_IN}' then l3_charge_amount
      else 0
      END as RHP_LD_calls
FROM
      Stage_RHP_CALLS  TAB1
left join
      elau_usg_maestro.ape1_rated_event  TAB2
ON
      TAB1.account = TAB2.customer_id
),

RHP_CALLS as
(
SELECT
        ENTERPRISE_ID,
        Max(CONSOLIDATED_IND) as CONSOLIDATED_IND   ,
        Max(IPTV)  as  IPTV,
        Max(ROG_IG_INTERNET) as ROG_IG_INTERNET,
        Max(RHP) as RHP ,
        Max(REVENUE_IPTV) as REVENUE_IPTV  ,
        Max(REVENUE_ROG_IG_INT) as REVENUE_ROG_IG_INT,
        Max(REVENUE_RHP) as REVENUE_RHP ,
        Max(DISCOUNT_IPTV) as DISCOUNT_IPTV ,
        Max(DISCOUNT_ROG_IG_INT) as DISCOUNT_ROG_IG_INT ,
        Max(DISCOUNT_RHP)  as DISCOUNT_RHP,
        Max(TENURE_MONTH_Iptv) as TENURE_MONTH_Iptv ,
        Max(TENURE_MONTH_INT) as TENURE_MONTH_INT ,
        Max(TENURE_MONTH_RHP) as TENURE_MONTH_RHP ,
        Max(Age) as Age,
        Max(postal_code) as postal_code,
        Max(FLG_IN_CABLEFOOTPRINT) as FLG_IN_CABLEFOOTPRINT,
        Max(FLG_OUT_CABLEFOOTPRINT) as FLG_OUT_CABLEFOOTPRINT ,
        Max(INTERNET_REV_GR_IG) as INTERNET_REV_GR_IG ,
        Max(TV_REV_GR_IG) as TV_REV_GR_IG ,
        Max(RHP_REV_GR_IG) as RHP_REV_GR_IG,
        Max(INTERNET_MLY_USAGE_ACTUAL) as INTERNET_MLY_USAGE_ACTUAL,
        Max(TMN)as TMN,
        Max(THEMEPACK)as  THEMEPACK ,
        Max(internet_mly_usage_entitled_ig) as internet_mly_usage_entitled_ig   ,
        Max(flg_int_10to19mbps) as flg_int_10to19mbps ,
        SUM(RHP_Local_calls) as RHP_Local_calls ,
        SUM(RHP_LD_calls) as RHP_LD_calls
FROM
     Pre_RHP_CALLS
GROUP BY enterprise_id
)

insert OVERWRITE TABLE anchor_segment.anchor_segment_table  PARTITION(PROCESS_DATE)
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
     internet_mly_usage_entitled_ig ,
     flg_int_10to19mbps,
         RHP_Local_calls,
         RHP_LD_calls ,
     '${hiveconf:RUN_DATE_IN}'
from RHP_CALLS;

