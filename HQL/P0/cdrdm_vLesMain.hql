DROP VIEW IF EXISTS cdrdm.vLesMain;
CREATE VIEW cdrdm.vLesMain AS
SELECT 
LES_DATA.subject_number,
LES_DATA.call_type,
LES_DATA.call_date,
LES_DATA.calling_number,
LES_DATA.called_number,
LES_DATA.calling_serial_number,
LES_DATA.called_serial_number,
LES_DATA.imsi,
LES_DATA.original_called_number,
LES_DATA.redirecting_number,
LES_DATA.chargeable_duration,
LES_DATA.First_Cell_complete_Orig AS First_Cell_complete,
LES_DATA.Last_Cell_complete_Orig AS Last_Cell_complete,
LES_DATA.record_seq_number,
LES_DATA.switch_name,
LES_DATA.first_cell_id_extension,
LES_DATA.subscriptionType, 
LES_DATA.SRVCCIndicator, 
--FCSite.file_name AS First_cell_file,
LES_DATA.call_timestamp_date,
--LES_DATA.local_subscriber_date,
FCSite.Site AS First_cell_site,
FCSite.Cell AS First_cell_cell,
FCSite.ANTENNA_TY AS First_cell_ANTENNA_TY,
FCSite.AZIMUTH AS First_cell_AZIMUTH,
FCSite.BEAMWIDTH AS First_cell_BEAMWIDTH,
FCSite.Site_name AS First_cell_site_name, 
FCSite.City AS First_cell_site_City_name,
FCSite.Province AS First_cell_site_Prov_Name,
FCSite.Address AS First_cell_site_Address,
FCSite.X AS First_cell_site_X,
FCSite.Y AS First_cell_site_Y,
FCSite.Latitude AS First_cell_site_Latitude,
FCSite.Longitude AS First_cell_site_Longitude,
FCSite.CGI AS First_Cell,
--LCSite.file_name  AS Last_cell_file,
LCSite.Site AS Last_cell_site,
LCSite.Cell AS Last_cell_cell,
LCSite.ANTENNA_TY AS Last_cell_ANTENNA_TY,
LCSite.AZIMUTH AS Last_cell_AZIMUTH,
LCSite.BEAMWIDTH AS Last_cell_BEAMWIDTH,
LCSite.Site_name AS Last_cell_site_name, 
LCSite.City AS Last_cell_site_City_name,
LCSite.Province AS Last_cell_site_Prov_Name,
LCSite.Address AS Last_cell_site_Address,
LCSite.X AS Last_cell_site_X,
LCSite.Y AS Last_cell_site_Y,
LCSite.Latitude AS Last_cell_site_Latitude,
LCSite.Longitude AS Last_cell_site_Longitude,
LCSite.CGI AS Last_Cell,
--FCSiteL.File_Name AS FNF,
--LCSiteL.File_Name AS FNL,
CASE 
  WHEN
  (LES_DATA.imsi BETWEEN 302720392000000 AND 302720392999999 OR-- HSPA
               LES_DATA.imsi BETWEEN 302720592000000 AND 302720592999999 OR-- LTE
   LES_DATA.imsi BETWEEN 302720692000000 AND 302720692999999)  -- LTE
              AND 
              (SUBSTR(LES_DATA.First_Cell_complete,7,4) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
              OR SUBSTR(LES_DATA.Last_Cell_complete ,7,4) IN  ('8791','87F5','FCBC','FD20','FCC1','FD25'))
        THEN 'D'
  
 
    WHEN        (SUBSTR(LES_DATA.First_Cell_complete,7,4) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
          OR SUBSTR(LES_DATA.Last_Cell_complete ,7,4) IN  ('8791','87F5','FCBC','FD20','FCC1','FD25'))
        THEN 'R'
  ELSE NULL
  END AS IsTbaytel
FROM 
 (
  SELECT  
  v_les_hist_all.subject_number  AS  subject_number,
  call_type,
  v_les_hist_all.call_timestamp  AS  call_date,
  v_les_hist_all.calling_number  AS  calling_number,
  v_les_hist_all.called_number  AS  called_number,
  v_les_hist_all.calling_serial_number  AS  calling_serial_number,
  v_les_hist_all.called_serial_number  AS  called_serial_number,
  v_les_hist_all.imsi  AS  imsi,
  v_les_hist_all.original_called_number  AS  original_called_number,
  v_les_hist_all.redirecting_number  AS  redirecting_number,
  v_les_hist_all.chargeable_duration  AS  chargeable_duration,
  CASE WHEN (v_les_hist_all.first_cell_hist = 0) THEN first_cell_curr_formatted  ELSE FC_Hist.LTE_HSPA_CellSite END   AS  First_Cell_complete,
  CASE WHEN (v_les_hist_all.last_cell_hist = 0) THEN Last_cell_curr_formatted  ELSE  LC_HIST.LTE_HSPA_CellSite END   AS  Last_Cell_complete,
  CASE WHEN (v_les_hist_all.first_cell_hist = 0) THEN first_cell_curr  ELSE FC_Hist.LTE_HSPA_CellSite_Orig END   AS  First_Cell_complete_Orig,
  CASE WHEN (v_les_hist_all.last_cell_hist = 0) THEN Last_cell_curr ELSE  LC_HIST.LTE_HSPA_CellSite_Orig END   AS  Last_Cell_complete_Orig,
  v_les_hist_all.record_seq_number  AS  record_seq_number,
  COALESCE(switch_id_curr, network_element, NULL) AS switch_name,
  v_les_hist_all.first_cell_id_extension,
  v_les_hist_all.subscriptionType, 
   v_les_hist_all.SRVCCIndicator,
   v_les_hist_all.call_timestamp_date
--   v_les_hist_all.local_subscriber_date
  FROM       (
   SELECT
   subscriber_no AS subject_number,
   CAST(CASE 
    WHEN  call_type = 2 THEN 'GSM Mobile Originating'
    WHEN  call_type = 3 THEN 'GSM Roaming Call Forward'
    WHEN  call_type = 4 THEN 'GSM Call Forward'
    WHEN  call_type = 5 THEN 'GSM Mobile Terminating'
    WHEN  call_type = 6 THEN 'GSM Originating SMS'
    WHEN  call_type = 8 THEN 'GSM Terminating SMS'
    ELSE 'Unknown' 
    END AS VARCHAR(40)) AS call_type,                                   
      CASE
	  WHEN call_type IN (2,6) THEN TRIM(CAST(imei AS CHAR(16)))
	  WHEN file_name="FACT_GSM_OUT_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16)))
	  WHEN file_name="FACT_GSM_OUT_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16)))
	  ELSE NULL
	  END AS calling_serial_number,
      CASE
	  WHEN call_type IN (5,8) THEN TRIM(CAST(imei AS CHAR(16)))	
	  WHEN file_name="FACT_GSM_IN_VOICE_HIST" THEN TRIM(CAST(imei AS CHAR(16)))
	  WHEN file_name="FACT_GSM_IN_SMS_HIST" THEN TRIM(CAST(imei AS CHAR(16)))
      ELSE NULL 
	  END AS called_serial_number,
      imsi AS imsi,
      call_timestamp AS call_timestamp,
      chargeable_duration AS chargeable_duration,
      cleansed_calling_number AS calling_number,
      CASE
	  WHEN call_type = 4 THEN cleansed_redirecting_number 
      WHEN file_name="FACT_GSM_IN_VOICE_HIST" OR file_name="FACT_GSM_IN_SMS_HIST" THEN subscriber_no
--      WHEN file_name="fact_gsm_out_voice_hist" THEN 
	  WHEN file_name="FACT_GSM_CF_HIST" THEN CAST (redirecting_number AS BIGINT)
	  ELSE cleansed_called_number
      END AS called_number,
      cleansed_original_number AS original_called_number,
      CASE
	  WHEN call_type = 4 THEN cleansed_called_number
	  WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN CAST(redirecting_number AS BIGINT)
      ELSE cleansed_redirecting_number
      END AS redirecting_number,
      SUBSTR(exchange_identity, 1, (INSTR( exchange_identity, '_') -1 )) AS switch_id_curr,
      CASE
	  WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN switch_name
	  ELSE CAST('0' AS STRING)	  
	  END AS switch_id_hist,
      
	  CASE
	  WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18))
	  ELSE
	  (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN CONCAT(SUBSTR(first_cell_id,1,6),
                     regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+", ""),
                     regexp_replace(TRIM(SUBSTR(first_cell_id,11,4)), "^0+", ""))  
               ELSE  CONCAT(SUBSTR(first_cell_id,1,6),
                     regexp_replace(TRIM(SUBSTR(first_cell_id,7,4)), "^0+",""),
                     regexp_replace(TRIM(first_cell_id_extension), "^0+", ""), 
                     SUBSTR(TRIM(first_cell_id),11,4)) END)
	   
      END AS first_cell_curr_formatted,
	CASE
    WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18))
	ELSE
	(CASE WHEN first_cell_id IS NULL OR TRIM(first_cell_id) ='' THEN first_cell_id 
      ELSE
		(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(first_cell_id)
		ELSE CONCAT(SUBSTR(TRIM(first_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(first_cell_id),11,4)) END) END)
	END  AS first_cell_curr,

	CASE
	WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN COALESCE(first_cell_id,'0')
	ELSE CAST('0' AS CHAR(14))
	END AS first_cell_hist,
	
    CASE
	WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST","FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) 
	ELSE	
	(CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' 
             THEN CONCAT(SUBSTR(last_cell_id,1,6),
                     regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+", ""),
                     regexp_replace(TRIM(SUBSTR(last_cell_id,11,4)), "^0+", "" ))
               ELSE  CONCAT(SUBSTR(last_cell_id,1,6),
                     regexp_replace(TRIM(SUBSTR(last_cell_id,7,4)), "^0+",""),
                     regexp_replace(TRIM(first_cell_id_extension), "^0+", ""),
                      SUBSTR(TRIM(last_cell_id),11,4)) END)
    END AS last_cell_curr_formatted,
 
    CASE
    WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST", "FACT_GSM_CF_HIST", "FACT_GSM_OUT_SMS_HIST", "FACT_GSM_IN_SMS_HIST") THEN CAST('0' AS VARCHAR(18)) 
	ELSE
    (CASE WHEN last_cell_id IS NULL OR TRIM(last_cell_id) ='' THEN last_cell_id 
      ELSE (CASE WHEN first_cell_id_extension IS NULL OR TRIM(first_cell_id_extension) ='' THEN TRIM(last_cell_id)
             ELSE CONCAT(SUBSTR(TRIM(last_cell_id),1,10),TRIM(first_cell_id_extension),SUBSTR(TRIM(last_cell_id),11,4)) END) END)
    END AS last_cell_curr,

	CASE
	WHEN file_name IN ("FACT_GSM_IN_VOICE_HIST", "FACT_GSM_OUT_VOICE_HIST") THEN COALESCE(last_cell_id, '0')
	ELSE CAST('0' AS CHAR(14))
	END AS last_cell_hist,
	
      CAST(NULL AS BIGINT) record_seq_number,
      1  AS source_type,
      'cdrdm.fact_gsm_cdr'  AS source_table,
      first_cell_id_extension AS first_cell_id_extension,
       subscriptionType, 
       SRVCCIndicator,
	   call_timestamp_date
--	   CAST (NULL AS STRING) AS local_subscriber_date
	   
	   
---------------------------------
	   FROM cdrdm.fact_gsm_cdr WHERE (call_type IS NULL OR call_type <> 3) AND file_name NOT IN ('FACT_GSM_CFR_HIST')
----------------------------------     
       
-- FACT_GSM_CDR end here

   UNION ALL
-- FACT_SMS_CDR starts here       

   SELECT
   
   subscriber_no AS subject_number,
   CASE
   WHEN file_name="FACT_SMS_HISTORY" THEN CAST((CASE WHEN charged_party = 'O' THEN 'IUM Originating SMS Hist'  ELSE 'IUM Terminating SMS Hist' END) AS VARCHAR(40))
   ELSE
   CAST((CASE WHEN charged_party = 'O' THEN 'IUM Originating SMS'  ELSE 'IUM Terminating SMS' END) AS VARCHAR(40)) END AS 
   call_type,
   
   CAST(NULL AS VARCHAR(16)) AS calling_serial_number,
   CAST(NULL AS VARCHAR(16)) AS called_serial_number,
   imsi AS imsi,
   local_subscriber_timestamp AS call_timestamp,
--date_1 AS call_timestamp,
   CAST(NULL AS BIGINT) AS chargeable_duration,
   CASE WHEN charged_party = 'O' THEN subscriber_no 
        ELSE other_msisdn
--  ELSE CAST(TRIM(LEADING '1' FROM TRIM(LEADING '0' FROM CAST((other_msisdn( FORMAT '9999999999999999' ))AS CHAR(16))))AS BIGINT) 
        END AS calling_number,
   CASE WHEN charged_party = 'T' THEN subscriber_no 
   ELSE other_msisdn  
--  ELSE CAST(TRIM(LEADING '1' FROM TRIM(LEADING '0' FROM CAST((other_msisdn ( FORMAT '9999999999999999' )) AS CHAR(16)))) AS BIGINT)
   END AS called_number,
   CAST(NULL AS BIGINT) AS original_called_number,
   CAST(NULL AS BIGINT) AS redirecting_number,
   CAST(NULL AS CHAR(15)) AS switch_id_curr,
   switch_name AS switch_id_hist,
   CAST('0' AS VARCHAR(18)) AS first_cell_curr_formatted,
   CAST('0' AS VARCHAR(18)) AS first_cell_curr,
   CAST('0' AS CHAR(14)) AS first_cell_hist,
   CAST('0' AS VARCHAR(18)) AS last_cell_curr_formatted,
   CAST('0' AS VARCHAR(18)) AS last_cell_curr,
   CAST('0' AS CHAR(14)) AS last_cell_hist,
   CAST(NULL AS BIGINT) AS record_seq_number,
   3 AS source_type,
   'cdrdm.fact_sms_cdr'  AS source_table,
   CAST(NULL AS VARCHAR(4))  AS first_cell_id_extension,
   CAST (NULL AS CHAR(4)) AS subscriptionType, 
    CAST (NULL AS CHAR(2)) AS SRVCCIndicator,
--   CAST (NULL AS STRING) AS call_timestamp_date,	
-- Giving partition column local_subscriber_date same alias as the call_timestamp_date column -- of other union tables
   local_subscriber_date AS call_timestamp_date
   
  FROM cdrdm.fact_sms_cdr
  
  UNION ALL
 
   SELECT
   cleansed_calling_number AS subject_number,
   CAST(CONCAT('Transit CDR - ',(CASE  WHEN route_id IN ('DACCI', 'STRHI') THEN route_id ELSE 'VMR'  END)) AS VARCHAR(40)) AS call_type,
   CAST(NULL AS VARCHAR(16)) AS calling_serial_number,
   CAST(NULL AS VARCHAR(16)) AS called_serial_number,
   CAST(NULL AS BIGINT) imsi,
   call_timestamp AS call_timestamp,
   chargeable_duration AS chargeable_duration,
   cleansed_calling_number AS calling_number,
   cleansed_called_number AS called_number,
   cleansed_original_number AS original_called_number,
   cleansed_redirecting_number AS redirecting_number,
   SUBSTR(exchange_identity, 1, (instr( exchange_identity, '_') -1 )) AS switch_id_curr,
   CAST('0' AS STRING) AS switch_id_hist,
   CAST('0' AS VARCHAR(18)) AS first_cell_curr_formatted,
    CAST('0' AS CHAR(18)) AS first_cell_curr,
   CAST('0' AS CHAR(14)) AS first_cell_hist,
   CAST('0' AS VARCHAR(18)) AS last_cell_curr_formatted,
   CAST('0' AS VARCHAR(18)) AS last_cell_curr,
   CAST('0' AS CHAR(14)) AS last_cell_hist,
   record_sequence_number AS record_seq_number,
   1  AS source_type,
   'fact_gsm_transit_cdr    '  AS source_table,
   CAST(NULL AS VARCHAR(4))  AS first_cell_id_extension,
    CAST(NULL AS CHAR(4)) subscriptionType, -- CHAR(4) 
    CAST (NULL AS CHAR(2)) AS SRVCCIndicator,
	call_timestamp_date
--	CAST (NULL AS STRING) AS local_subscriber_date
   
   FROM cdrdm.fact_gsm_transit_cdr X1,
    (
    SELECT route_id FROM ela_v21.route_gg WHERE route_type ='R'
    UNION ALL
    SELECT 'DACCI' AS route_id 
    UNION ALL
    SELECT 'STRHI' AS route_id 	
    ) Y
   WHERE X1.incoming_route_id = Y.route_id
   
-- FACT_SMS_CDR ends here
  
    ) v_les_hist_all
	LEFT OUTER JOIN cdrdm.v_measured_object  FC_Hist ON (FC_Hist.measured_object_id = v_les_hist_all.first_cell_hist)
  LEFT OUTER JOIN  cdrdm.v_measured_object LC_Hist ON (LC_Hist.measured_object_id = v_les_hist_all.last_cell_hist)
  LEFT OUTER JOIN cdrdm.dim_network_element DNE ON (DNE.network_element_id = v_les_hist_all.switch_id_hist)
	
) LES_Data    
-- LEFT OUTER JOIN cdrdm.les_join_dim_cell_site_info_temp FCSite ON LES_data.First_Cell_complete = FCSite.cgi_hex AND to_date(call_date) = FCSite.file_date
--LEFT OUTER JOIN cdrdm.les_join_dim_cell_site_info_temp LCSite ON LES_data.Last_Cell_complete = LCSite.cgi_hex AND to_date(call_date)=LCSite.file_date
--****************************************************


LEFT OUTER JOIN (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) AS rank_file_date FROM cdrdm.DIM_CELL_SITE_INFO) X WHERE X.rank_file_date=1) FCSite ON LES_data.First_Cell_complete = FCSite.cgi_hex

LEFT OUTER JOIN (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY cgi_hex ORDER BY file_date DESC) AS rank_file_date FROM cdrdm.DIM_CELL_SITE_INFO) X WHERE X.rank_file_date=1) LCSite ON LES_data.Last_Cell_complete = LCSite.cgi_hex;
