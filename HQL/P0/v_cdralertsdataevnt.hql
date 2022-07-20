SET hive.variable.substitute=true;

SET EXTRACT_DATE;
SET EXTRACT_DATE_NO_HYPEN;

CREATE OR REPLACE VIEW cdrdm.v_cdralertsdataevnt_${hiveconf:EXTRACT_DATE_NO_HYPEN}
AS
Select DT, FM_IMSI, first_cell_curr_formatted, last_cell_curr_formatted, Sufix_IMSI, pdate
from 
(
Select cast(concat (record_opening_date , ' ', record_opening_time) as varchar(20)) Dt,
       cast(served_imsi as varchar(20)) FM_IMSI,
       (Case 

            WHEN Nvl(wireless_generation,'X') = 'X' 

            THEN  Concat (NVL(PLMN_ID,'302720'), 
                          REGEXP_REPLACE(TRIM(Nvl(Tracking_area_code,'')), "^0+", ''), 
                          REGEXP_REPLACE(TRIM(Nvl(Eutran_cellid,'')), "^0+", ''))

            WHEN wireless_generation IN ('3G','2G') AND (Nvl(Location_area_code,'X') <> 'X' and 
                                                         Nvl(Cell_Id,'X') <> 'X' and  
                                                         TRIM(Location_area_code) <> '' and 
                                                         TRIM(cell_id) <> '' and 
                                                         trim(Cell_Id) <> '0000' and 
                                                         Trim(Location_area_code) <> '0000') 
            THEN  Concat (NVL(PLMN_ID,'302720'), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Location_area_code,'')), "^0+", ''), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Cell_ID,'')), "^0+", '') 
                          )

            WHEN wireless_generation IN ('4G','5G') AND (Nvl(Tracking_area_code,'X') <> 'X' and 
                                                         Nvl(Eutran_cellid,'X') <> 'X' and  
                                                         TRIM(Tracking_area_code) <> '' and 
                                                         TRIM(Eutran_cellid) <> '' and 
                                                         trim(Eutran_cellid) <> '00000000' and 
                                                         Trim(Tracking_area_code) <> '0000') 
            THEN  Concat (NVL(PLMN_ID,'302720'), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Tracking_area_code,'')), "^0+", ''), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Eutran_cellid,'')), "^0+", '') 
                          )

            WHEN wireless_generation IN ('3G','2G') AND (Nvl(Location_area_code,'X') = 'X' OR 
                                                         Nvl(Cell_Id,'X') = 'X' OR  
                                                         TRIM(Location_area_code) = '' OR 
                                                         TRIM(cell_id) = '' OR 
                                                         trim(Cell_Id) = '0000' OR 
                                                         Trim(Location_area_code) = '0000') 
            THEN  Concat (NVL(PLMN_ID,'302720'), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Tracking_area_code,'')), "^0+", ''), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Eutran_cellid,'')), "^0+", '') 
                          )

            WHEN wireless_generation IN ('4G','5G') AND (Nvl(Tracking_area_code,'X') = 'X' OR 
                                                         Nvl(Eutran_cellid,'X') = 'X' OR  
                                                         TRIM(Tracking_area_code) = '' OR 
                                                         TRIM(Eutran_cellid) = '' OR 
                                                         trim(Eutran_cellid) = '00000000' OR 
                                                         Trim(Tracking_area_code) = '0000') 
            THEN  Concat (NVL(PLMN_ID,'302720'), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Location_area_code,'')), "^0+", ''), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Cell_Id,'')), "^0+", '') 
                          )

       Else 'C'
       End) first_cell_curr_formatted,
       '' as last_cell_curr_formatted,
       Substr(trim(cast(served_imsi as Varchar(20))),length(cast(served_imsi as Varchar(20)))-1,2) Sufix_IMSI,
       record_opening_date pdate

from cdrdm.fact_gprs_cdr
where 
gprs_choice_mask_archive <> '21' 
and plmn_id in ('302720', '302370', '302320','302820')
and record_opening_date between '${hiveconf:EXTRACT_DATE}' and '${hiveconf:EXTRACT_DATE}'

Union all

Select Cast(triggerTime as varchar(20)) as dt, 
        Cast(drvd_IMSI as varchar(20)) FM_IMSI, 

        (Case        
           WHEN Gy_RAT_Type <> '6' AND (Nvl(Gy_Location_Area_Code,'X') <> 'X' and 
                                                         Nvl(Gy_Cell_Identity,'X') <> 'X' and  
                                                         TRIM(Gy_Location_Area_Code) <> '' and 
                                                         TRIM(Gy_Cell_Identity) <> '' and 
                                                         trim(Gy_Cell_Identity) <> '0000' and 
                                                         Trim(Gy_Location_Area_Code) <> '0000') 
            THEN  Concat (NVL(Gy_Location_MCC_MNC,'302720'), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Gy_Location_Area_Code,'')), "^0+", ''), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Gy_Cell_Identity,'')), "^0+", '') 
                          )

          
           WHEN Gy_RAT_Type <> '6' AND (Nvl(Gy_Location_Area_Code,'X') <> 'X' and 
                                                         Nvl(Gy_Service_Area_Code,'X') <> 'X' and  
                                                         TRIM(Gy_Location_Area_Code) <> '' and 
                                                         TRIM(Gy_Service_Area_Code) <> '' and 
                                                         trim(Gy_Service_Area_Code) <> '0000' and 
                                                         Trim(Gy_Location_Area_Code) <> '0000') 
            THEN  Concat (NVL(Gy_Location_MCC_MNC,'302720'), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Gy_Location_Area_Code,'')), "^0+", ''), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Gy_Service_Area_Code,'')), "^0+", '') 
                          )

           WHEN Gy_RAT_Type = '6' AND (Nvl(Gy_Tracking_Area_Code,'X') <> 'X' and 
                                                         Nvl(Gy_ECID,'X') <> 'X' and  
                                                         TRIM(Gy_Tracking_Area_Code) <> '' and 
                                                         TRIM(Gy_ECID) <> '' and 
                                                         trim(Gy_ECID) <> '0000' and 
                                                         Trim(Gy_Tracking_Area_Code) <> '0000') 
            THEN  Concat (NVL(Gy_Location_MCC_MNC,'302720'), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Gy_Tracking_Area_Code,'')), "^0+", ''), '-',
                          REGEXP_REPLACE(TRIM(Nvl(Gy_ECID,'')), "^0+", '') 
                          )

          Else 'C'
         End) first_cell_curr_formatted,
         
        ' ' as last_cell_curr_formatted,
        Substr(trim(cast(drvd_IMSI as Varchar(20))),length(cast(drvd_IMSI as Varchar(20)))-1,2) Sufix_IMSI,
        triggerdate pdate
      
from  cdrdm.fact_chatr_occ_cdr
where 
Gy_Location_MCC_MNC in ('302720', '302370', '302320','302820')
and triggerdate between '${hiveconf:EXTRACT_DATE}' and '${hiveconf:EXTRACT_DATE}'
) U_DATA_CDR
where nvl(first_cell_curr_formatted,'X') <> 'X' 
and pdate  between '${hiveconf:EXTRACT_DATE}' and '${hiveconf:EXTRACT_DATE}'
;
