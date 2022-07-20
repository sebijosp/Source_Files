DROP VIEW IF EXISTS cdrdm.V_RIT_sum_UpDownBytes_CTN;
CREATE VIEW cdrdm.V_RIT_sum_UpDownBytes_CTN AS
SELECT
record_opening_date AS UDate,
subscriber_no,
'DATA' AS UType,
CID,
SUM(CASE WHEN wireless_generation ='4G' 
THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0) ELSE 0 END) AS W4G,
SUM(CASE WHEN wireless_generation ='3G' 
THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0) ELSE 0 END) AS W3G,
SUM(CASE WHEN wireless_generation ='2G' 
THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0) ELSE 0 END) AS W2G,
SUM(CASE WHEN wireless_generation NOT IN ('4G','3G','2G')
THEN COALESCE(data_volume_uplink_archive,0) + COALESCE(data_volume_downlink_archive,0) ELSE 0 END) AS WOther,
COUNT(*) AS Cnt
FROM cdrdm.Fact_gprs_cdr_ext_Tbay WHERE LAC_TAC_Type ='T' AND IMSI_Type = 'R' AND gprs_choice_mask_archive = '18' GROUP BY record_opening_date,subscriber_no,'DATA',CID;