use cdrdm;
SET hive.execution.engine=tez;
SET hive.execution.engine=tez;
SET hive.variable.substitute=true;
SET hive.exec.parallel=true;
SET hive.tez.auto.reducer.parallelism=true;

use cdrdm;
SET WHERE_clause;

INSERT OVERWRITE TABLE cdrdm.fact_obroamrpts_cdr 
PARTITION(cday, service_type, call_distance_type)
SELECT
  srvcrequestdttmnrml_date 
    AS CALLDAY,
  DATE_FORMAT(SUBSTR(srvcrequestdttmnrml_date,1,10),'MMMMM') 
    AS CALLMONTH, 
  splitcdr.CALLSTARTTIME 
    AS CALLSTARTTIME,
  splitcdr.CALLENDTIME
    AS CALLENDTIME,
  CASE 
      WHEN SUBSTR(imsi,0,6) = '302720'
          THEN 'Rogers' 
      WHEN SUBSTR(imsi,0,6) = '302370'  
          THEN 'Fido'
      ELSE imsi 
  END 
    AS BRAND,
  NVL(TRIM(imsi),'?') 
    AS IMSI,
  NVL(TRIM(imei),'?') 
    AS IMEI,
  subscriberno 
    AS MSISDN,
  NVL(pll.plmn_tadig,'')  
    AS VPLMNTADIGCode,
  CASE 
    WHEN COALESCE(lst1accessdomain,
                  lst2accessdomain,
                  lst3accessdomain) = '20'
      THEN COALESCE(wificountry.country_name,
                    pll.country_name,
                    '?') 
    ELSE
      NVL(pll.country_name,'?') 
  END 
    AS VISITEDCOUNTRY,
  NVL(pll.plmn_name,'') 
    AS VPLMNNAME,
  CASE
    WHEN lst1accessdomain = '2'
      THEN
        CASE  
          WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,6),'-',''),'0')
          ELSE 
            REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,7),'-','')
        END
    WHEN lst2accessdomain = '2'
      THEN 
        CASE
          WHEN SUBSTR(lst2accessnetworkinfo,7,1) = '-' 
            THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,1,6),'-',''),'0')
          ELSE 
            REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,1,7),'-','')
        END
    WHEN lst3accessdomain = '2'
      THEN 
        CASE
          WHEN SUBSTR(lst3accessnetworkinfo,7,1) = '-' 
            THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,1,6),'-',''),'0')
          ELSE 
            REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,1,7),'-','')
        END
    ELSE ''
  END 
    AS MCCMNC,
  splitcdr.CALLTYPE
    AS CALLTYPE,
  splitcdr.SERVICE
      AS SERVICE,    
  CASE
    WHEN
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
      COALESCE(npanxx.country_name,
                specialnum.specialnumber,
                fivedigitprfx.country_name,
                fourdigitprfx.country_name,
                '?')
    ELSE
        COALESCE(threedigitprfx.country_name,
                twodigitprfx.country_name,
                onedigitprfx.country_name,
                '?')
  END 
    AS CALLEDCOUNTRY,
  CASE 
    WHEN 
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
        COALESCE(IF(ISNOTNULL(npanxx.country_name),'1',NULL),
                  IF(ISNOTNULL(specialnum.specialnumber),'1',NULL),
                  fivedigitprfx.plmn_country_code,
                  fourdigitprfx.plmn_country_code,
                  '?')
    ELSE 
        COALESCE(threedigitprfx.plmn_country_code,
                twodigitprfx.plmn_country_code,
                onedigitprfx.plmn_country_code,
                '?')
  END 
    AS COUNTRYCODE,    
  CAST(calledpartyaddress AS BIGINT) 
    AS CALLEDNUMBER,
  requestedpartyaddress 
    AS DIALEDNUMBER,
  splitcdr.CALLDURATIONSECS 
    AS CALLDURATIONSECS,
  splitcdr.CALLDURATIONMINS 
    AS CALLDURATIONMINS,
   '' 
    AS DATAVOLUME,
   '' 
    AS CALL_TYPE_LEVEL_2,
   '' 
    AS ACCESS_POINT_NAME_NI,
   '1' 
    AS NUMBEROFEVENTS,
  CASE
    WHEN lst1accessdomain = '2'
      THEN
        CASE
          WHEN 
            (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-','') = "" 
              OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
          WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-',''),16,10)
        END
    WHEN lst2accessdomain = '2'
      THEN
        CASE
          WHEN 
            (REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,14,7),'-','') = "" 
              OR REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
          WHEN SUBSTR(lst2accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,13,7),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,14,7),'-',''),16,10)
        END
    WHEN lst3accessdomain = '3'
      THEN
        CASE
          WHEN 
            (REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,14,7),'-','') = "" 
              OR REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
          WHEN SUBSTR(lst3accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,13,7),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,14,7),'-',''),16,10)
        END
    ELSE ''
  END 
    AS SERVINGCELLID,
  CASE
    WHEN lst1accessdomain = '2'
      THEN
        CASE
          WHEN (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-','') = "" 
                OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
          WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-',''),16,10)
        END
    WHEN lst2accessdomain = '2'
      THEN
        CASE
          WHEN (REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,9,4),'-','') = "" 
                OR REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
          WHEN SUBSTR(lst2accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,8,4),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst2accessnetworkinfo,9,4),'-',''),16,10)
          END
    WHEN lst3accessdomain = '3'
      THEN
        CASE
          WHEN (REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,9,4),'-','') = "" 
                OR REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
          WHEN SUBSTR(lst3accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,8,4),'-',''),16,10)
          ELSE
            CONV(REGEXP_REPLACE(SUBSTR(lst3accessnetworkinfo,9,4),'-',''),16,10)
          END
    ELSE ''
  END
    AS SERVINGLAC,
  CURRENT_DATE 
    AS INSERTDATE,
  CURRENT_TIMESTAMP 
    AS INSERTTIME,
  file_name 
    AS FILE_NAME,
  record_type 
    AS RECORD_TYPE,
  family_name 
    AS FAMILY_NAME,
  version_id 
    AS VERSION_ID,
  file_time 
    AS FILE_TIME,
  file_id 
    AS FILE_ID,
  switch_name 
    AS SWITCH_NAME,
 NULL as callcorrected,
  CAST(REGEXP_REPLACE(SUBSTR(srvcrequestdttmnrml_date,1,10),'-','') AS INT) 
    AS CDAY,
  '4G'
    AS SERVICE_TYPE,
  'ROAMING'
    AS CALL_DISTANCE_TYPE
FROM 
(
        SELECT
        srvcrequestdttmnrml_date,
        srvcdeliverystarttimestamp,
        srvcdeliveryendtimestamp,
        chargeableduration,
        lstofnrmlmediac1chgtime,
        lstofnrmlmediac2chgtime,
        lstofnrmlmediac3chgtime,
        lstofnrmlmediac4chgtime,
        lstofnrmlmediac5chgtime,
        lstofnrmlmediac1desctype1,
        lstofnrmlmediac2desctype1,
        lstofnrmlmediac2desctype2,
        lst1changetime,
        lst2changetime,
        imsi,
        imei,
        subscriberno,
        lst1accesstype,
        lst1accessnetworkinfo,
        lst1accessdomain,
        lst2accesstype,
        lst2accessnetworkinfo,
        lst2accessdomain,
        lst3accesstype,
        lst3accessnetworkinfo,
        lst3accessdomain,
        roleofnode,
        sipmethod,
        calledpartyaddress,
        requestedpartyaddress,
        file_name,
        record_type,
        family_name,
        version_id,
        file_time,
        file_id,
        switch_name
  FROM cdrdm.fact_imm_cdr
  WHERE
    sipmethod='INVITE'
    AND
    fa.srvcrequestdttmnrml_date='${WHERE_clause}'
    AND
    (
        (--ViLTE and VoLTE CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='2' AND
                lst1accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst1accessnetworkinfo,1,6) <> '302-72' 
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='2' AND
                lst2accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst2accessnetworkinfo,1,6) <> '302-72'
            )
            OR
            (
                lst3accesstype='1' AND
                lst2accessdomain='2' AND
                lst3accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst3accessnetworkinfo,1,6) <> '302-72'
            )
        )
        OR
        (--ViWiFi and VoWiFi CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='20' AND
                lst1accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst1accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='20' AND
                lst2accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst2accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst3accesstype='1' AND
                lst3accessdomain='20' AND
                lst3accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst3accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
        )
    )
) factcdr
LEFT OUTER JOIN 
(
    SELECT * FROM 
    (
            SELECT 
                PL.*,
                ROW_NUMBER() OVER(PARTITION BY pl.plmn_code ORDER BY pl.effective_date desc) AS rown  
            FROM 
                ela_v21.plmn_settlement_gg pl
    ) AS pll 
    WHERE pll.rown=1
)  pll 
ON IF ( 
        LENGTH(REGEXP_REPLACE(REGEXP_EXTRACT(
                COALESCE(
                    IF((SUBSTR(lst3accessnetworkinfo,1,6)='FFFFFF'),NULL,lst3accessnetworkinfo),
                    IF((SUBSTR(lst2accessnetworkinfo,1,6)='FFFFFF'),NULL,lst2accessnetworkinfo),
                    IF((SUBSTR(lst1accessnetworkinfo,1,6)='FFFFFF'),NULL,lst1accessnetworkinfo)),'(.+?-.+?)-.+?'),'-',''))=6,
        REGEXP_REPLACE(REGEXP_EXTRACT(
                COALESCE(
                    IF((SUBSTR(lst3accessnetworkinfo,1,6)='FFFFFF'),NULL,lst3accessnetworkinfo),
                    IF((SUBSTR(lst2accessnetworkinfo,1,6)='FFFFFF'),NULL,lst2accessnetworkinfo),
                    IF((SUBSTR(lst1accessnetworkinfo,1,6)='FFFFFF'),NULL,lst1accessnetworkinfo)),'(.+?-.+?)-.+?'),'-',''),
        CONCAT(REGEXP_REPLACE(REGEXP_EXTRACT(
                COALESCE(
                    IF((SUBSTR(lst3accessnetworkinfo,1,6)='FFFFFF'),NULL,lst3accessnetworkinfo),
                    IF((SUBSTR(lst2accessnetworkinfo,1,6)='FFFFFF'),NULL,lst2accessnetworkinfo),
                    IF((SUBSTR(lst1accessnetworkinfo,1,6)='FFFFFF'),NULL,lst1accessnetworkinfo)),'(.+?-.+?)-.+?'),'-',''),'0')
 )=pll.plmn_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
              WHERE LENGTH(ps.plmn_country_code) = 5
        ) AS cntry
        WHERE cntry.rown=1
) fivedigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fivedigitprfx.plmn_country_code
LEFT OUTER JOIN
  (
      SELECT
          cntry.plmn_country_code,
          cntry.country_name
      FROM
          (
              SELECT
                  DISTINCT ps.plmn_country_code,
                  ps.country_name,
                  row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
              FROM
                  ela_v21.plmn_settlement_gg ps
                WHERE LENGTH(ps.plmn_country_code) = 4
          ) AS cntry
          WHERE cntry.rown=1
  ) fourdigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fourdigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE ps.plmn_country_code <> '1'
                AND LENGTH(ps.plmn_country_code) = 3
        ) AS cntry
        WHERE cntry.rown=1
) threedigitprfx
ON SUBSTR(calledpartyaddress,1,3) = threedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 2
        ) AS cntry
        WHERE cntry.rown=1
)  twodigitprfx
ON SUBSTR(calledpartyaddress,1,2) = twodigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 1
        ) AS cntry
        WHERE cntry.rown=1
)  onedigitprfx
ON SUBSTR(calledpartyaddress,1,1) = onedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
  SELECT 
    pll.alpha2_cd AS alpha2_cd,
    pll.country_name AS country_name
  FROM
  (
        SELECT 
            DISTINCT TRIM(alpha2_cd) AS alpha2_cd,
            TRIM(country_name) AS country_name,
            ROW_NUMBER() OVER(
                PARTITION BY pl.alpha2_cd ORDER BY pl.effective_date desc
            ) AS rown  
        FROM ela_v21.plmn_settlement_gg pl
        WHERE 
          TRIM(alpha2_cd) IS NOT NULL 
          AND trim(alpha2_cd) <> ''
  ) AS pll
  WHERE pll.rown=1
)  wificountry 
ON regexp_extract(COALESCE(lst3accessnetworkinfo,lst2accessnetworkinfo,lst1accessnetworkinfo),'\;(.*)',1)=wificountry.alpha2_cd
LEFT OUTER JOIN
(
    SELECT 
        npnx.npa npa,
        npnx.nxx nxx,
        npnx.state_code state_code,
        CASE 
            WHEN sc.country_code = 'CAN' 
                THEN 'CANADA'
            WHEN sc.country_code = 'USA'
                THEN 'USA'
            ELSE '?'
        END country_name
    FROM 
        ela_v21.npa_nxx_gg npnx
    LEFT OUTER JOIN
    (
        SELECT state_code,
                country_code
        FROM cdrdm.dim_state_mps
    ) sc
    ON sc.state_code = npnx.state_code
)  npanxx
ON  (SUBSTR(calledpartyaddress,2,3) = npanxx.npa 
    AND SUBSTR(calledpartyaddress,5,3) = npanxx.nxx)
LEFT OUTER JOIN
(
    SELECT 
        DISTINCT spnm.npa npa,
        spnm.nxx nxx,
        'SPECIAL NUMBER' AS specialnumber 
    FROM 
        ela_v21.special_numbers_gg spnm
)  specialnum
ON  (SUBSTR(calledpartyaddress,2,3) = specialnum.npa
    AND SUBSTR(calledpartyaddress,5,3) = specialnum.nxx)
LEFT OUTER JOIN
(
  SELECT
    from_unixtime(CAST (mediaordomainvalue[0] AS BIGINT),'HH:mm:ss') 
      AS CALLSTARTTIME,
    CASE
      WHEN ((lstofnrmlmediac2chgtime IS NULL) AND (lst2changetime IS NULL))
            THEN
                from_unixtime(CAST (unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss') 
                            AS BIGINT),'HH:mm:ss') 
      WHEN ((lstofnrmlmediac2chgtime IS NOT NULL) OR (lst2changetime IS NOT NULL)) 
            THEN
                from_unixtime(CAST (COALESCE(LEAD(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
                            unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) 
                            AS BIGINT),'HH:mm:ss') 
      ELSE '?'
    END
      AS CALLENDTIME,
    CONCAT(IF(roleofnode='1','MTC ','MOC '),mediaordomainvalue[1])
      AS CALLTYPE,
    mediaordomainvalue[1] 
      AS SERVICE,
    CASE
       WHEN ((lstofnrmlmediac2chgtime IS NULL) AND (lst2changetime IS NULL))
            THEN
                CAST(
                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss') - 
                    unix_timestamp(srvcdeliverystarttimestamp,'yyyy-MM-dd HH:mm:ss') 
                    AS BIGINT
                    )
        WHEN ((lstofnrmlmediac2chgtime IS NOT NULL) OR (lst2changetime IS NOT NULL)) 
            THEN
                CAST(COALESCE(lead(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) - mediaordomainvalue[0] AS BIGINT) 
        ELSE '?'
    END
      AS CALLDURATIONSECS,
    CASE
       WHEN ((lstofnrmlmediac2chgtime IS NULL) AND (lst2changetime IS NULL))
            THEN
            CAST(CEIL((
                unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss') - 
                unix_timestamp(srvcdeliverystarttimestamp,'yyyy-MM-dd HH:mm:ss')
                )/60)
                AS BIGINT
                )
        WHEN ((lstofnrmlmediac2chgtime IS NOT NULL) OR (lst2changetime IS NOT NULL)) 
            THEN
                CAST(CEIL((COALESCE(lead(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) - mediaordomainvalue[0])/60) AS BIGINT) 
        ELSE '?'
    END
      AS CALLDURATIONMINS,
      mediaordomainvalue[1] AS type,
      mediaordomainkey,
      mediaordomainvalue[0] AS changestarts,
      COALESCE(lead(mediaordomainvalue[0],1,NULL) OVER (PARTITION BY file_name,roleofnode,srvcdeliverystarttimestamp,srvcdeliveryendtimestamp ORDER BY mediaordomainvalue[0]),
               unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')) 
        AS changeends,
      subscriberno AS splitsubscriberno,
      sipmethod AS splitsipmethod,
      roleofnode AS splitroleofnode,
      srvcrequestdttmnrml_date AS splitsrvcrequestdttmnrml_date,
      srvcdeliverystarttimestamp AS splitsrvcdeliverystarttimestamp,
      srvcdeliveryendtimestamp AS splitsrvcdeliveryendtimestamp,
      file_name AS splitfile_name
  FROM cdrdm.fact_imm_cdr splitimm
  LATERAL VIEW EXPLODE
    (
    map(    
            'lstofnrmlmediac1chgtime',
            ARRAY
            (
                CAST(unix_timestamp(lstofnrmlmediac1chgtime,'yyyy-MM-dd HH:mm:ss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    (
                                        lstofnrmlmediac1desctype1 = '1' OR 
                                        lstofnrmlmediac1desctype2 = '1' OR 
                                        lstofnrmlmediac1desctype3 = '1'
                                    )
                                    AND
                                    (
                                        lst1accessdomain = '2'
                                    )
                                )  
                                    THEN 'ViLTE'
                            WHEN( 
                                    (
                                        lstofnrmlmediac1desctype1 = '1' OR 
                                        lstofnrmlmediac1desctype2 = '1' OR 
                                        lstofnrmlmediac1desctype3 = '1'
                                    )
                                    AND
                                    lst1accessdomain = '20'
                                )
                                    THEN 'ViWiFi'
                            WHEN( 
                                    lstofnrmlmediac1desctype1 = '0'
                                    AND
                                    lst1accessdomain = '2'
                                )
                                THEN 'VoLTE'
                            WHEN( 
                                    lstofnrmlmediac1desctype1 = '0'
                                    AND
                                    lst1accessdomain = '20'
                                )
                                THEN 'VoWiFi'
                            ELSE '?'                        
                        END 
                        AS STRING)
            ),
            'lstofnrmlmediac2chgtime',
            ARRAY
            (
                CAST(unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    ( 
                                        lstofnrmlmediac2desctype1 = '1' OR 
                                        lstofnrmlmediac2desctype2 = '1' OR 
                                        lstofnrmlmediac2desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '2'
                                        )
                                    )
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    ( 
                                        lstofnrmlmediac2desctype1 = '1' OR 
                                        lstofnrmlmediac2desctype2 = '1' OR 
                                        lstofnrmlmediac2desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '20'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '20'
                                        )
                                    )                                
                                )
                                THEN 'ViWiFi'
                            WHEN( 
                                    lstofnrmlmediac2desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '2'
                                        )
                                    )
                                )
                                THEN 'VoLTE'
                            WHEN( 
                                    lstofnrmlmediac2desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst1accessdomain = '20'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst2accessdomain = '20'
                                        )
                                    )
                                )
                                THEN 'VoWiFi'
                            ELSE '?'
                        END 
                        AS STRING)
            ),
            'lst2changetime',
            ARRAY
            (
                CAST(unix_timestamp(lst2changetime,'yyyyMMddHHmmss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    ( 
                                        (
                                            ( 
                                                lstofnrmlmediac1desctype1 = '1' OR 
                                                lstofnrmlmediac1desctype2 = '1' OR 
                                                lstofnrmlmediac1desctype3 = '1'
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR 
                                                lstofnrmlmediac2desctype2 = '1' OR 
                                                lstofnrmlmediac2desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '2' 
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    ( 
                                        (
                                            ( 
                                                lstofnrmlmediac1desctype1 = '1' OR 
                                                lstofnrmlmediac1desctype2 = '1' OR 
                                                lstofnrmlmediac1desctype3 = '1'
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR 
                                                lstofnrmlmediac2desctype2 = '1' OR 
                                                lstofnrmlmediac2desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '20' 
                                )
                                THEN 'ViWiFi'
                            WHEN(
                                    ( 
                                        (
                                            lstofnrmlmediac1desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '2' 
                                )
                                THEN 'VoLTE'
                            WHEN(
                                    ( 
                                        (
                                            lstofnrmlmediac1desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst2changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac2chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac2chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst2changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                        )
                                    )
                                    AND 
                                    lst2accessdomain = '20' 
                                )
                                THEN 'VoWiFi'
                            ELSE '?'
                        END
                        AS STRING)
            ),
            'lstofnrmlmediac3chgtime',
            ARRAY
            (
                CAST(unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') AS STRING),
                CAST(
                        CASE 
                            WHEN(
                                    ( 
                                        lstofnrmlmediac3desctype1 = '1' OR 
                                        lstofnrmlmediac3desctype2 = '1' OR 
                                        lstofnrmlmediac3desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                    
                                            )
                                            AND 
                                            lst2accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst3accessdomain = '2'
                                        )
                                    )
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    ( 
                                        lstofnrmlmediac3desctype1 = '1' OR 
                                        lstofnrmlmediac3desctype2 = '1' OR 
                                        lstofnrmlmediac3desctype3 = '1'
                                    )
                                    AND
                                    (
                                        (
                                            unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                            COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                    unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                    )                                    
                                        )
                                        AND 
                                        lst2accessdomain = '20'
                                    )
                                    OR
                                    (
                                        (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                        )
                                        AND
                                        lst3accessdomain = '20'
                                    )
                                )
                                THEN 'ViWiFi'
                            WHEN( 
                                    lstofnrmlmediac3desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst2accessdomain = '2'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst3accessdomain = '2'           
                                        )
                                    )
                                )
                                THEN 'VoLTE'
                            WHEN( 
                                    lstofnrmlmediac3desctype1 = '0'
                                    AND
                                    (
                                        (
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') < 
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                            AND
                                            lst2accessdomain = '20'
                                        )
                                        OR
                                        (
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                            AND
                                            lst3accessdomain = '20'
                                        )                                        
                                    )
                                )
                                THEN 'VoWiFi'
                            ELSE '?'
                        END 
                        AS STRING)
            ),
            'lst3changetime',
            ARRAY
            (
                CAST(unix_timestamp(lst3changetime,'yyyyMMddHHmmss') AS STRING),
                CAST(
                        CASE
                            WHEN(
                                    ( 
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR
                                                lstofnrmlmediac2desctype2 = '1' OR
                                                lstofnrmlmediac2desctype3 = '1'   
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac3desctype1 = '1' OR 
                                                lstofnrmlmediac3desctype2 = '1' OR 
                                                lstofnrmlmediac3desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '2' 
                                )
                                THEN 'ViLTE'
                            WHEN(
                                    (
                                        (
                                            ( 
                                                lstofnrmlmediac2desctype1 = '1' OR 
                                                lstofnrmlmediac2desctype2 = '1' OR 
                                                lstofnrmlmediac2desctype3 = '1'
                                            )                        
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            ( 
                                                lstofnrmlmediac3desctype1 = '1' OR 
                                                lstofnrmlmediac3desctype2 = '1' OR 
                                                lstofnrmlmediac3desctype3 = '1'
                                            )
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )                                  
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '20' 
                                )
                                THEN 'ViWiFi'
                            WHEN(
                                    ( 
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss') 
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac3desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '2'
                                )
                                THEN 'VoLTE'
                            WHEN(
                                    (
                                        (
                                            lstofnrmlmediac2desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lst3changetime,'yyyyMMddHHmmss') <
                                                unix_timestamp(COALESCE(lstofnrmlmediac3chgtime,srvcdeliveryendtimestamp),'yyyy-MM-dd HH:mm:ss')
                                            )
                                        )
                                        OR
                                        (
                                            lstofnrmlmediac3desctype1 = '0'
                                            AND
                                            (
                                                unix_timestamp(lstofnrmlmediac3chgtime,'yyyy-MM-dd HH:mm:ss') <
                                                COALESCE(unix_timestamp(lst3changetime,'yyyyMMddHHmmss'),
                                                        unix_timestamp(srvcdeliveryendtimestamp,'yyyy-MM-dd HH:mm:ss')
                                                        )
                                            )
                                        )
                                    )
                                    AND
                                    lst3accessdomain = '20'
                                ) 
                                THEN 'VoWiFi'
                            ELSE '?'
                        END
                        AS STRING)
            )
        )
    ) exp AS mediaordomainkey,mediaordomainvalue
  WHERE
    srvcrequestdttmnrml_date='${WHERE_clause}'
    AND
    sipmethod='INVITE'
    AND
    mediaordomainvalue[0] IS NOT NULL
    AND
    (
        (--ViLTE and VoLTE CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='2' AND
                lst1accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst1accessnetworkinfo,1,6) <> '302-72' 
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='2' AND
                lst2accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst2accessnetworkinfo,1,6) <> '302-72'
            )
            OR
            (
                lst3accesstype='1' AND
                lst2accessdomain='2' AND
                lst3accessnetworkinfo LIKE '%\-%' AND
                SUBSTR(lst3accessnetworkinfo,1,6) <> '302-72'
            )
        )
        OR
        (--ViWiFi and VoWiFi CDRs
            (
                lst1accesstype='1' AND
                lst1accessdomain='20' AND
                lst1accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst1accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst2accesstype='1' AND
                lst2accessdomain='20' AND
                lst2accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst2accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
            OR
            (
                lst3accesstype='1' AND
                lst3accessdomain='20' AND
                lst3accessnetworkinfo LIKE '%\;%' AND
                regexp_extract(lst3accessnetworkinfo,'\;(.*)',1) <> 'CA'
            )
        )
  )
  ORDER BY changestarts ASC
) splitcdr
ON
( 
      srvcrequestdttmnrml_date = splitcdr.splitsrvcrequestdttmnrml_date AND
      subscriberno = splitcdr.splitsubscriberno AND
      sipmethod = splitcdr.splitsipmethod AND
      roleofnode = splitcdr.splitroleofnode AND 
      srvcdeliverystarttimestamp = splitcdr.splitsrvcdeliverystarttimestamp AND
      srvcdeliveryendtimestamp = splitcdr.splitsrvcdeliveryendtimestamp AND
      file_name = splitcdr.splitfile_name
);

INSERT INTO TABLE cdrdm.fact_obroamrpts_cdr 
PARTITION(cday, service_type, call_distance_type)
SELECT
  srvcrequestdttmnrml_date 
    AS CALLDAY,
  DATE_FORMAT(SUBSTR(srvcrequestdttmnrml_date,1,10),'MMMMM') 
    AS CALLMONTH, 
  SUBSTR(srvcdeliverystarttimestamp,12,8) 
    AS CALLSTARTTIME,
  SUBSTR(srvcdeliveryendtimestamp,12,8)
    AS CALLENDTIME,
  CASE 
      WHEN SUBSTR(imsi,0,6) = '302720'
          THEN 'Rogers' 
      WHEN SUBSTR(imsi,0,6) = '302370'  
          THEN 'Fido'
      ELSE imsi 
  END 
    AS BRAND,
  NVL(TRIM(imsi),'?') 
    AS IMSI,
  NVL(TRIM(imei),'?') 
    AS IMEI,
  subscriberno 
    AS MSISDN,
  NVL(pll.plmn_tadig,'')  
    AS VPLMNTADIGCode,
  NVL(pll.country_name,'?') 
    AS VISITEDCOUNTRY,
  NVL(pll.plmn_name,'') 
    AS VPLMNNAME,
    CASE  
        WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
        THEN CONCAT(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,6),'-',''),'0')
        ELSE 
        REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,1,7),'-','')
    END
    AS MCCMNC,
  CONCAT(IF(roleofnode='1','MTC ','MOC '),'SMS')
    AS CALLTYPE,
  'SMS' 
    AS SERVICE,    
  CASE
    WHEN
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
      COALESCE(npanxx.country_name,
                specialnum.specialnumber,
                fivedigitprfx.country_name,
                fourdigitprfx.country_name,
                '?')
    ELSE
        COALESCE(threedigitprfx.country_name,
                twodigitprfx.country_name,
                onedigitprfx.country_name,
                '?')
  END 
    AS CALLEDCOUNTRY,
  CASE 
    WHEN 
        SUBSTR(calledpartyaddress,1,LENGTH(calledpartyaddress)-10) = '1' 
                OR calledpartyaddress LIKE '1%'
    THEN 
        COALESCE(IF(ISNOTNULL(npanxx.country_name),'1',NULL),
                  IF(ISNOTNULL(specialnum.specialnumber),'1',NULL),
                  fivedigitprfx.plmn_country_code,
                  fourdigitprfx.plmn_country_code,
                  '?')
    ELSE 
        COALESCE(threedigitprfx.plmn_country_code,
                twodigitprfx.plmn_country_code,
                onedigitprfx.plmn_country_code,
                '?')
  END 
    AS COUNTRYCODE,    
  CAST(calledpartyaddress AS BIGINT) 
    AS CALLEDNUMBER,
  requestedpartyaddress 
    AS DIALEDNUMBER,
  '' 
    AS CALLDURATIONSECS,
  '' 
    AS CALLDURATIONMINS,
   '' 
    AS DATAVOLUME,
   '' 
    AS CALL_TYPE_LEVEL_2,
   '' 
    AS ACCESS_POINT_NAME_NI,
   '1' 
    AS NUMBEROFEVENTS,
    CASE
        WHEN 
        (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-','') = "" 
            OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-','') = "") 
            THEN  ''
        WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,13,7),'-',''),16,10)
        ELSE
        CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,14,7),'-',''),16,10)
    END
    AS SERVINGCELLID,
    CASE
        WHEN (REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-','') = "" 
            OR REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-','') = "") 
            THEN ''
        WHEN SUBSTR(lst1accessnetworkinfo,7,1) = '-' 
            THEN CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,8,4),'-',''),16,10)
        ELSE
        CONV(REGEXP_REPLACE(SUBSTR(lst1accessnetworkinfo,9,4),'-',''),16,10)
    END
    AS SERVINGLAC,
  CURRENT_DATE 
    AS INSERTDATE,
  CURRENT_TIMESTAMP 
    AS INSERTTIME,
  file_name 
    AS FILE_NAME,
  record_type 
    AS RECORD_TYPE,
  family_name 
    AS FAMILY_NAME,
  version_id 
    AS VERSION_ID,
  file_time 
    AS FILE_TIME,
  file_id 
    AS FILE_ID,
  switch_name 
    AS SWITCH_NAME,
  CAST(REGEXP_REPLACE(SUBSTR(srvcrequestdttmnrml_date,1,10),'-','') AS INT) 
    AS CDAY,
  '4G'
    AS SERVICE_TYPE,
  'ROAMING'
    AS CALL_DISTANCE_TYPE
FROM 
(
        SELECT
        srvcrequestdttmnrml_date,
        srvcdeliverystarttimestamp,
        srvcdeliveryendtimestamp,
        chargeableduration,
        imsi,
        imei,
        subscriberno,
        lst1accesstype,
        lst1accessnetworkinfo,
        lst1accessdomain,
        roleofnode,
        sipmethod,
        calledpartyaddress,
        requestedpartyaddress,
        file_name,
        record_type,
        family_name,
        version_id,
        file_time,
        file_id,
        switch_name
  FROM cdrdm.fact_imm_cdr
  WHERE
    srvcrequestdttmnrml_date='${WHERE_clause}'
    AND
    sipmethod='MESSAGE'
    AND
    lst1accessdomain = '2' -- Include messages in LTE zone only
    AND
    SUBSTR(lst1accessnetworkinfo,1,6) <> '302-72' -- Exclude Rogers
    AND
    regexp_extract(lst1accessnetworkinfo,'\;(.*)',1) <> 'CA' -- Exclude CANADA roamers
    AND
    lst1accessnetworkinfo <> '000-000-0000-0' -- Exclude unknown MCC-MNC-LAC-CellID
    AND
    lst1accessnetworkinfo <> '---' -- Exclude unknown locations
) factcdr
LEFT OUTER JOIN 
(
    SELECT * FROM 
    (
            SELECT 
                PL.*,
                ROW_NUMBER() OVER(PARTITION BY pl.plmn_code ORDER BY pl.effective_date desc) AS rown  
            FROM 
                ela_v21.plmn_settlement_gg pl
    ) AS pll 
    WHERE pll.rown=1
)  pll 
ON IF ( 
        LENGTH(REGEXP_REPLACE(REGEXP_EXTRACT(lst1accessnetworkinfo,'(.+?-.+?)-.+?'),'-',''))=6,
        REGEXP_REPLACE(REGEXP_EXTRACT(lst1accessnetworkinfo,'(.+?-.+?)-.+?'),'-',''),
        CONCAT(REGEXP_REPLACE(REGEXP_EXTRACT(lst1accessnetworkinfo,'(.+?-.+?)-.+?'),'-',''),'0')
 )=pll.plmn_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
              WHERE LENGTH(ps.plmn_country_code) = 5
        ) AS cntry
        WHERE cntry.rown=1
) fivedigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fivedigitprfx.plmn_country_code
LEFT OUTER JOIN
  (
      SELECT
          cntry.plmn_country_code,
          cntry.country_name
      FROM
          (
              SELECT
                  DISTINCT ps.plmn_country_code,
                  ps.country_name,
                  row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
              FROM
                  ela_v21.plmn_settlement_gg ps
                WHERE LENGTH(ps.plmn_country_code) = 4
          ) AS cntry
          WHERE cntry.rown=1
  ) fourdigitprfx
ON SUBSTR(calledpartyaddress,1,4) = fourdigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE ps.plmn_country_code <> '1'
                AND LENGTH(ps.plmn_country_code) = 3
        ) AS cntry
        WHERE cntry.rown=1
) threedigitprfx
ON SUBSTR(calledpartyaddress,1,3) = threedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 2
        ) AS cntry
        WHERE cntry.rown=1
)  twodigitprfx
ON SUBSTR(calledpartyaddress,1,2) = twodigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT
        cntry.plmn_country_code,
        cntry.country_name
    FROM
        (
            SELECT
                DISTINCT ps.plmn_country_code,
                ps.country_name,
                row_number() OVER (PARTITION BY ps.plmn_country_code ORDER BY effective_date DESC) AS rown
            FROM
                ela_v21.plmn_settlement_gg ps
            WHERE   UPPER(country_name) NOT LIKE 'US%' 
                AND UPPER(country_name) NOT like '%CANADA%' 
                AND UPPER(country_name) NOT like 'UNITED STATES%'
                AND LENGTH(ps.plmn_country_code) = 1
        ) AS cntry
        WHERE cntry.rown=1
)  onedigitprfx
ON SUBSTR(calledpartyaddress,1,1) = onedigitprfx.plmn_country_code
LEFT OUTER JOIN
(
    SELECT 
        npnx.npa npa,
        npnx.nxx nxx,
        npnx.state_code state_code,
        CASE 
            WHEN sc.country_code = 'CAN' 
                THEN 'CANADA'
            WHEN sc.country_code = 'USA'
                THEN 'USA'
            ELSE '?'
        END country_name
    FROM 
        ela_v21.npa_nxx_gg npnx
    LEFT OUTER JOIN
    (
        SELECT state_code,
                country_code
        FROM cdrdm.dim_state_mps
    ) sc
    ON sc.state_code = npnx.state_code
)  npanxx
ON  (SUBSTR(calledpartyaddress,2,3) = npanxx.npa 
    AND SUBSTR(calledpartyaddress,5,3) = npanxx.nxx)
LEFT OUTER JOIN
(
    SELECT 
        DISTINCT spnm.npa npa,
        spnm.nxx nxx,
        'SPECIAL NUMBER' AS specialnumber 
    FROM 
        ela_v21.special_numbers_gg spnm
)  specialnum
ON  (SUBSTR(calledpartyaddress,2,3) = specialnum.npa
    AND SUBSTR(calledpartyaddress,5,3) = specialnum.nxx);
