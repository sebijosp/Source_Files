DROP VIEW IF EXISTS cdrdm.fact_imm_cdr_ext_tbay;
CREATE VIEW IF NOT EXISTS cdrdm.fact_imm_cdr_ext_tbay PARTITIONED ON (srvcrequestdttmnrml_date) AS
SELECT 
        (
            CASE 
                WHEN 
                (
                    CASE
                        WHEN 
                        (
                            IMSI BETWEEN 302720392000000 AND 302720392999999
                            OR
                            IMSI BETWEEN 302720592000000 AND 302720592999999
                            OR
                            IMSI BETWEEN 302720692000000 AND 302720692999999
                        )
                        THEN 'T' 
                        ELSE 
                        (
                            CASE
                                WHEN find_in_set(SUBSTR(IMSI,1,6), '302720,302370,302820') > 0
                                    THEN 'R'
                                ELSE 'O'
                            END
                        )
                    END
                ) = 'T'
                AND
                (
                    CASE
                            WHEN
                                (
                                    (
                                        lst1accessnetworkinfo IS NULL 
                                        OR
                                        TRIM(lst1accessnetworkinfo) =''
                                    ) 
                                    AND
                                    (
                                        lst2accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst2accessnetworkinfo) =''
                                    )
                                    AND
                                    (
                                        lst3accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst3accessnetworkinfo) =''
                                    )
                                )
                                    THEN NULL
                            WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'T'
                             WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'R'
                            ELSE 'O'
                    END
                ) = 'T'
                THEN 'D'
                WHEN 
                (
                    CASE
                        WHEN 
                            (
                                IMSI BETWEEN 302720392000000 AND 302720392999999 
                                OR
                                IMSI BETWEEN 302720592000000 AND 302720592999999
                                OR
                                IMSI BETWEEN 302720692000000 AND 302720692999999
                            ) 
                        THEN 'T' 
                        ELSE 
                            (
                                CASE
                                    WHEN find_in_set(SUBSTR(IMSI,1,6), '302720,302370,302820') > 0
                                        THEN 'R' 
                                    ELSE 'O'
                                END
                            )
                    END
                ) = 'O' 
                AND 
                (
                    CASE
                            WHEN
                                (
                                    (
                                        lst1accessnetworkinfo IS NULL 
                                        OR
                                        TRIM(lst1accessnetworkinfo) =''
                                    ) 
                                    AND
                                    (
                                        lst2accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst2accessnetworkinfo) =''
                                    )
                                    AND
                                    (
                                        lst3accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst3accessnetworkinfo) =''
                                    )
                                )
                                    THEN NULL
                            WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'T'
                             WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'R'
                            ELSE 'O'
                    END
                ) = 'T'
                THEN 'O'
                WHEN 
                (
                    CASE
                        WHEN
                            (
                                IMSI BETWEEN 302720392000000 AND 302720392999999
                                OR
                                IMSI BETWEEN 302720592000000 AND 302720592999999
                                OR
                                IMSI BETWEEN 302720692000000 AND 302720692999999
                            )
                        THEN 'T' 
                        ELSE
                        (
                            CASE
                                WHEN find_in_set(SUBSTR(IMSI,1,6), '302720,302370,302820') > 0 
                                    THEN 'R' 
                                ELSE 'O'
                            END
                        )
                    END
                ) = 'R'
                AND 
                (
                    CASE
                            WHEN
                                (
                                    (
                                        lst1accessnetworkinfo IS NULL 
                                        OR
                                        TRIM(lst1accessnetworkinfo) =''
                                    ) 
                                    AND
                                    (
                                        lst2accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst2accessnetworkinfo) =''
                                    )
                                    AND
                                    (
                                        lst3accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst3accessnetworkinfo) =''
                                    )
                                )
                                    THEN NULL
                            WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'T'
                             WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'R'
                            ELSE 'O'
                    END
                ) = 'T'
                THEN 'R'
                ELSE NULL
            END 
        )
        AS IsTbayTel,
        (
            CASE 
                WHEN
                (
                    IMSI BETWEEN 302720392000000 AND 302720392999999 -- HSPA
                    OR
                    IMSI BETWEEN 302720592000000 AND 302720592999999 -- LTE
                    OR
                    IMSI BETWEEN 302720692000000 AND 302720692999999 -- LTE
                )
                THEN 'T'
                ELSE
                (
                    CASE
                        WHEN SUBSTR(IMSI,1,6) IN ('302720','302370','302820') 
                        THEN 'R'
                        ELSE 'O'
                    END
                )
            END
        ) 
        AS IMSI_Type,
        (
            CASE
                WHEN
                    (
                        (
                            lst1accessnetworkinfo IS NULL 
                            OR
                            TRIM(lst1accessnetworkinfo) =''
                        ) 
                        AND
                        (
                            lst2accessnetworkinfo IS NULL
                            OR
                            TRIM(lst2accessnetworkinfo) =''
                        )
                        AND
                        (
                            lst3accessnetworkinfo IS NULL
                            OR
                            TRIM(lst3accessnetworkinfo) =''
                        )
                    )
                    THEN NULL
                WHEN
                    (
                        (
                            UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                            OR
                            UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                            OR
                            UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                        )
                        AND
                        (
                            SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                            OR
                            SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                            OR
                            SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                        )
                    )
                    THEN 'T'
                WHEN
                    (
                        (
                            (
                                UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                OR
                                UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                OR
                                UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                            )
                            AND
                            (
                                SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                OR
                                SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                OR
                                SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                            )
                        )
                        OR
                        ( lst1accessdomain = '20' OR lst2accessdomain = '20' OR lst3accessdomain = '20')
                    )
                    THEN 'R'
                    ELSE 'O'
            END
        ) 
        AS LAC_TAC_Type,
        (
            CASE 
                WHEN 
                (
                    CASE
                            WHEN
                                (
                                    (
                                        lst1accessnetworkinfo IS NULL 
                                        OR
                                        TRIM(lst1accessnetworkinfo) =''
                                    ) 
                                    AND
                                    (
                                        lst2accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst2accessnetworkinfo) =''
                                    )
                                    AND
                                    (
                                        lst3accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst3accessnetworkinfo) =''
                                    )
                                )
                                    THEN NULL
                            WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'T'
                             WHEN
                                (
                                    (
                                        (
                                            UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                            OR
                                            UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                            OR
                                            UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        )
                                        AND
                                        (
                                            SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                            OR
                                            SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                            OR
                                            SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                    )
                                    OR
                                    ( lst1accessdomain = '20' OR lst2accessdomain = '20' OR lst3accessdomain = '20')
                                )                              
                                    THEN 'R'
                            ELSE 'O'
                    END
                ) = 'T' 
                AND
                UPPER(SUBSTR(COALESCE(lst1accessnetworkinfo,lst2accessnetworkinfo,lst3accessnetworkinfo),9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                THEN UPPER(REGEXP_REPLACE(COALESCE(lst1accessnetworkinfo,lst2accessnetworkinfo,lst3accessnetworkinfo),"-",""))
                WHEN
                (
                    CASE
                            WHEN
                                (
                                    (
                                        lst1accessnetworkinfo IS NULL 
                                        OR
                                        TRIM(lst1accessnetworkinfo) =''
                                    ) 
                                    AND
                                    (
                                        lst2accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst2accessnetworkinfo) =''
                                    )
                                    AND
                                    (
                                        lst3accessnetworkinfo IS NULL
                                        OR
                                        TRIM(lst3accessnetworkinfo) =''
                                    )
                                )
                                    THEN NULL
                            WHEN
                                (
                                    (
                                        UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        OR
                                        UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                        OR
                                        UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                    )
                                    AND
                                    (
                                        SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                        OR
                                        SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                )
                                    THEN 'T'
                             WHEN
                                (
                                    (
                                        (
                                            UPPER(SUBSTR(lst1accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                            OR
                                            UPPER(SUBSTR(lst2accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25')
                                            OR
                                            UPPER(SUBSTR(lst3accessnetworkinfo,9,4)) NOT IN ('8791','87F5','FCBC','FD20','FCC1','FD25') 
                                        )
                                        AND
                                        (
                                            SUBSTR(lst1accessnetworkinfo,1,7) = '302-720'
                                            OR
                                            SUBSTR(lst2accessnetworkinfo,1,7) = '302-720'
                                            OR
                                            SUBSTR(lst3accessnetworkinfo,1,7) = '302-720'
                                        )
                                    )
                                    OR
                                    ( lst1accessdomain = '20' OR lst2accessdomain = '20' OR lst3accessdomain = '20')
                                ) 
                                    THEN 'R'
                            ELSE 'O'
                    END
                ) IN ('R','O') 
                THEN UPPER(REGEXP_REPLACE(COALESCE(lst1accessnetworkinfo,lst2accessnetworkinfo,lst3accessnetworkinfo),"-",""))
                ELSE NULL
            END
        )
        AS CID,
fimm.*
FROM cdrdm.fact_imm_cdr fimm;
