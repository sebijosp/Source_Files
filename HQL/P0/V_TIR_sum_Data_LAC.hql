-- show view cdr_prod_view.V_TIR_sum_Data_LAC;
DROP VIEW IF EXISTS cdrdm.V_TIR_sum_Data_LAC;
CREATE VIEW cdrdm.V_TIR_sum_Data_LAC AS 
SELECT Udate, subscriber_no, 'DATA' AS UType, LAC, Description, 
SUM(CASE WHEN wireless_generation ='4G' 
                        THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
                        ELSE 0 
                END)/1048576.00000000  W4G ,
     SUM(CASE WHEN wireless_generation ='3G' 
                        THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
                        ELSE 0 
                END)/1048576.00000000 W3G ,
     SUM(CASE WHEN wireless_generation ='2G' 
                        THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
                        ELSE 0 
                END)/1048576.00000000 W2G ,
     SUM(CASE WHEN wireless_generation NOT IN ('4G','3G','2G')
                        THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0)
                        ELSE 0 
                END)/1048576.00000000 WOther ,
          COUNT(*) Cnt
FROM

(SELECT record_opening_date AS UDate, subscriber_no, 
          SUBSTR(CID,7,4) AS LAC, wireless_generation, data_volume_uplink_archive, data_volume_downlink_archive
     
FROM   cdrdm.Fact_gprs_cdr_ext_Tbay
WHERE LAC_TAC_Type ='R' 
AND IMSI_Type = 'T'
AND gprs_choice_mask_archive = '18') X
LEFT OUTER JOIN
ela_v21.V_MPS_CONFIG_DETAILS_GG Y
ON X.LAC = Y.hex_inp_val_str
GROUP BY UDate, subscriber_no, LAC, Description;