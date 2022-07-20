DROP VIEW IF EXISTS cdrdm.V_Tbay_DT_LD_CNLD_Agg_R28;
CREATE VIEW cdrdm.V_Tbay_DT_LD_CNLD_Agg_R28 AS 
SELECT UDate, RID, Route_Desc,
          SUM(LD_US_VMIN) LD_US_VMIN,  
          SUM(LD_CANADA_VMIN) LD_CANADA_VMIN, 
          SUM(LD_INTL_VMIN) LD_INTL_VMIN, 
          SUM(LD_INTL_SMS_Cnt) LD_INTL_SMS_Cnt,
          COUNT(*) Cnt	
FROM           
(
SELECT to_date(channel_seizure_dt) AS UDATE, 
          (CASE WHEN CALL_ACTION_CODE = 1 THEN out_route ELSE in_route END) AS RID,

           (CASE WHEN TRIM(imsi) <> 'ZZZZZZZZZZZZZZZ'
                    THEN

                            (CASE WHEN 
                                           (imsi BETWEEN 302720392000000 AND 302720392999999 OR -- HSPA
                                           imsi BETWEEN 302720592000000 AND 302720592999999 OR -- LTE
                                           imsi BETWEEN 302720692000000 AND 302720692999999)  -- LTE
                           THEN IMSI
                            ELSE 'NON-TBT'
                            END)
      ELSE 'NON-TBT'
            END) AS IMSI_TYPE,
			
			-- TRIM not needed here as the data appears to be properly formatted, but still keeping it in case
          (CASE  WHEN TRIM(toll_type) = '3' AND TRIM(units_to_rate_uom) = 'MM' THEN rounded_units ELSE 0 END) AS LD_US_VMIN,
--          (CASE  WHEN TRIM(toll_type) = '3' AND TRIM(UNITS_TO_RATE_UOM) ='OC' THEN rounded_units ELSE 0 END) AS LD_US_SMS_Cnt,


          (CASE  WHEN TRIM(toll_type) = '4' AND TRIM(units_to_rate_uom) = 'MM' THEN rounded_units ELSE 0 END) AS LD_CANADA_VMIN,
--          (CASE  WHEN TRIM(toll_type) = '4' AND TRIM(UNITS_TO_RATE_UOM) ='OC' THEN rounded_units ELSE 0 END) AS LD_CANADA_SMS_Cnt,


          (CASE  WHEN TRIM(toll_type) = '5' AND TRIM(units_to_rate_uom) = 'MM' THEN rounded_units ELSE 0 END) AS LD_INTL_VMIN,
          
          (CASE  WHEN TRIM(units_to_rate_uom) ='OC' THEN rounded_units ELSE 0 END) AS LD_INTL_SMS_Cnt
		 -- units_to_rate_uom, toll_type, cycle_code, GG_BIGDATA_OP_TYPE


FROM cdrdm.v_fact_billed_usage_cdr
WHERE
TRIM(units_to_rate_uom) IN ('MM', 'OC') AND 
toll_type IN ('3','4','5') AND 
cycle_code = 35

) X 

LEFT OUTER JOIN (SELECT Route_ID, Route_Desc FROM ela_v21.route_gg WHERE route_type = 'C') Y
        ON X.RID = Y.Route_ID

GROUP BY UDate, RID, Route_Desc;