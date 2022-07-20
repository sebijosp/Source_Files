SET hive.variable.substitute=true;


SET EXTRACT_DATE;
SET EXTRACT_DATE_NO_HYPEN;

CREATE OR REPLACE VIEW cdrdm.v_cdralertsvoicesmsevnt_${hiveconf:EXTRACT_DATE_NO_HYPEN} 
AS
Select DT, FM_IMSI, first_cell_curr_formatted, last_cell_curr_formatted, Sufix_IMSI, pdate 
from 
(
      Select  cast(call_timestamp as Varchar(20)) as DT, 
              trim(cast(imsi as Varchar(20))) as FM_IMSI,
              Cast((case when NVL(first_cell_id_extension,'') = '' or trim(first_cell_id_extension) = '' 
                then
                      CONCAT(
                              SUBSTR(first_cell_id,1,3),'-',
                              SUBSTR(first_cell_id,4,3),'-',
                              REGEXP_REPLACE(Substr(Trim(first_cell_id),7,4), "^0+", ''),
                              '-',
                              REGEXP_REPLACE(Substr(Trim(first_cell_id),11,4), "^0+", '')
                            )
                else
                      CONCAT(
                              SUBSTR(first_cell_id,1,3),'-',
                              SUBSTR(first_cell_id,4,3),'-',
                              REGEXP_REPLACE(Substr(Trim(first_cell_id),7,4), "^0+", ''),
                              '-',
                              REGEXP_REPLACE(Trim(first_cell_id_extension), "^0+", ''),
                              Substr(Trim(first_cell_id),11,4)
                            )
                end) as Varchar(20)) as first_cell_curr_formatted,
                Cast ('---' as Varchar(20)) LAST_CELL_INTREM,
                Cast( CONCAT(
                          SUBSTR(last_cell_id,1,3),'-',
                          SUBSTR(last_cell_id,4,3),'-',
                          REGEXP_REPLACE(Substr(Trim(last_cell_id),7,4), "^0+", ''),
                          '-',
                          REGEXP_REPLACE(Substr(Trim(last_cell_id),11,4), "^0+", '')
                        ) as Varchar(20)) last_cell_curr_formatted,
                Substr(trim(cast(imsi as Varchar(20))),length(cast(imsi as Varchar(20)))-1,2) Sufix_IMSI,
                call_timestamp_date pdate
      from cdrdm.fact_gsm_cdr      
      where 
      CALL_TYPE  IN (2,5,6,8) 
      and call_timestamp_date between '${hiveconf:EXTRACT_DATE}' and '${hiveconf:EXTRACT_DATE}'
      and substr(trim(cast(imsi as Varchar(20))),1,6) in ('302720', '302370', '302320','302820')
      and subscriptionType <> '77'  
      and teleServiceCode <> 12  

      union all

      SELECT  CAST(SrvcRequestDttmNrml AS VARCHAR(20)) AS DT,
              cast(IMSI AS VARCHAR(20)) FM_IMSI, 
              cast(upper(Lst1AccessNetworkInfo) AS VARCHAR(20)) AS first_cell_curr_formatted,
              Cast ('---' as Varchar(20)) LAST_CELL_INTREM,
              CAST(upper(Lst2AccessNetworkInfo) aS VARCHAR(20)) AS last_cell_curr_formatted,
              Substr(trim(cast(imsi as Varchar(20))),length(cast(imsi as Varchar(20)))-1,2) Sufix_IMSI,
              srvcrequestdttmnrml_date pdate
      From cdrdm.FACT_IMM_CDR IMM
      WHERE (SIPMETHOD = 'INVITE' or SIPMETHOD = 'MESSAGE')
       and srvcrequestdttmnrml_date between '${hiveconf:EXTRACT_DATE}' and '${hiveconf:EXTRACT_DATE}' 
       and substr(trim(cast(imsi as Varchar(20))),1,6) in ('302720', '302370', '302320','302820')

      union all
      SELECT  CAST(SrvcRequestDttmNrml AS VARCHAR(20)) AS DT,
              cast(IMSI AS VARCHAR(20)) FM_IMSI, 
              cast(Lst1AccessNetworkInfo AS VARCHAR(20)) AS first_cell_curr_formatted,
              Cast ('---' as Varchar(20)) LAST_CELL_INTREM,
              CAST(Lst2AccessNetworkInfo aS VARCHAR(20)) AS last_cell_curr_formatted,
              Substr(trim(cast(imsi as Varchar(20))),length(cast(imsi as Varchar(20)))-1,2) Sufix_IMSI,
              srvcrequestdttmnrml_date p_date
      From cdrdm.FACT_UCC_CDR UCC
      WHERE (SIPMETHOD = 'INVITE' or SIPMETHOD = 'MESSAGE')
       and srvcrequestdttmnrml_date  between '${hiveconf:EXTRACT_DATE}' and '${hiveconf:EXTRACT_DATE}'
       and substr(trim(cast(imsi as Varchar(20))),1,6) in ('302720', '302370', '302320','302820')
) U_VM_CDR where nvl(last_cell_curr_formatted,'X') <> 'X' or nvl(first_cell_curr_formatted,'X') <> 'X'
and pdate between '${hiveconf:EXTRACT_DATE}' and '${hiveconf:EXTRACT_DATE}'
;
