-- show view cdr_prod_view.V_TIT_sum_UpDownBytes_CTN;
DROP VIEW IF EXISTS cdrdm.V_TIT_sum_UpDownBytes_CTN;
CREATE VIEW cdrdm.V_TIT_sum_UpDownBytes_CTN AS 
SELECT record_opening_date AS UDate, subscriber_no, 
          'DATA' AS UType,
     SUM(CASE WHEN wireless_generation ='4G' 
              THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
              ELSE 0 
          END)  W4G ,
     SUM(CASE WHEN wireless_generation ='3G' 
              THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
              ELSE 0 
        END) W3G ,
     SUM(CASE WHEN wireless_generation ='2G' 
              THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
              ELSE 0 
         END) W2G ,
     SUM(CASE WHEN wireless_generation NOT IN ('4G','3G','2G')
              THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
              ELSE 0 
        END) WOther ,
          COUNT(*) Cnt
FROM   cdrdm.Fact_gprs_cdr_ext_Tbay
WHERE LAC_TAC_Type ='T' 
AND IMSI_Type = 'T'
AND gprs_choice_mask_archive = '18'
GROUP BY record_opening_date, subscriber_no;