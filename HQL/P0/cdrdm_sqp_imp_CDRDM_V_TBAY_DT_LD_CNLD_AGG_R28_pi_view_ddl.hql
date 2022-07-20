use cdrdm;
DROP VIEW IF EXISTS v_tbay_dt_ld_cnld_agg_r28;
CREATE VIEW v_tbay_dt_ld_cnld_agg_r28 AS
SELECT
UDate,
RID,
Y.route_desc,
SUM(LD_US_VMIN) LD_US_VMIN,
SUM(LD_CANADA_VMIN) LD_CANADA_VMIN,
SUM(LD_INTL_VMIN) LD_INTL_VMIN,
SUM(LD_INTL_SMS_Cnt) LD_INTL_SMS_Cnt,
COUNT(*) Cnt
FROM
(SELECT to_date(a.channel_seizure_dt) AS UDate,
(CASE WHEN a.call_action_code = 1 THEN a.out_route ELSE a.in_route END) AS RID,
(CASE WHEN TRIM(a.imsi) <> 'ZZZZZZZZZZZZZZZ' THEN
(CASE WHEN
(a.imsi BETWEEN 302720392000000 AND 302720392999999 OR -- HSPA
a.imsi BETWEEN 302720592000000 AND 302720592999999 OR -- LTE
a.imsi BETWEEN 302720692000000 AND 302720692999999)  -- LTE
THEN a.imsi ELSE 'NON-TBT' END) ELSE 'NON-TBT' END) AS IMSI_TYPE,
(CASE  WHEN TRIM(a.toll_type) = '3' AND TRIM(a.units_to_rate_uom) = 'MM' THEN a.rounded_units ELSE 0 END) AS LD_US_VMIN,
(CASE  WHEN TRIM(a.toll_type) = '4' AND TRIM(a.units_to_rate_uom) = 'MM' THEN a.rounded_units ELSE 0 END) AS LD_CANADA_VMIN,
(CASE  WHEN TRIM(a.toll_type) = '5' AND TRIM(a.units_to_rate_uom) = 'MM' THEN a.rounded_units ELSE 0 END) AS LD_INTL_VMIN,
(CASE  WHEN TRIM(a.units_to_rate_uom) ='OC' THEN a.rounded_units ELSE 0 END) AS LD_INTL_SMS_Cnt
FROM elau_usage.usage_billed a
WHERE TRIM(a.units_to_rate_uom) IN ('MM', 'OC') AND
a.toll_type IN ('3','4','5')) X
LEFT OUTER JOIN (SELECT b.route_id, b.route_desc FROM ela_v21.route_gg b WHERE b.route_type = 'C') Y ON X.RID = Y.Route_ID
GROUP BY UDate, RID, route_desc;
