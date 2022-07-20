-- Business Rules
-----------------
-- Voice, Video over LTE and WiFi only
-- Pure and Hand-off scenarios
-- LD and Roaming only
-- Active Subscribers during the call period
--
-- CANADA
--       Circuit switch
--          MOC            => Report
--          MTC            => Do not report
--       VoLTE
--          MOC            => Report
--          MTC            => Do not report
-- USA
--       Circuit switch
--          MOC            => Do not report
--          MTC            => Report
--       VoLTE
--          MOC            => Report
--          MTC            => Report
-- INTERNATIONAL 
--       Circuit switch         
--          MOC            => Do not report
--          MTC            => Report
--       VoLTE
--          MOC            => Report
--          MTC            => Report

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=orc;
SET hive.execution.engine=tez;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.groupby.enabled=true;
SET hive.exec.parallel=true;

ALTER TABLE cdrdm.fact_volte_ldrpts_cdr DROP IF EXISTS PARTITION(month='${cur_partition_month}');
MSCK REPAIR TABLE cdrdm.fact_volte_ldrpts_cdr;
!echo "Truncated cdrdm.fact_volte_ldrpts_cdr table partition";
INSERT OVERWRITE TABLE cdrdm.dim_volte_ldrpt_months PARTITION(month) VALUES ('dummy','${cur_partition_month}');

INSERT INTO TABLE cdrdm.fact_volte_ldrpts_cdr_temp PARTITION(month)
SELECT
    NVL(TRIM(at.description),'?') AS ACCOUNT_DESC,
    NVL(TRIM(pt.product_desc),'?') AS PRODUCT_DESC,
    NVL(TRIM(ds.platform),'?') AS PLATFORM,
    CASE 
         WHEN ds.franchise_tp = 'R' 
             THEN 'Rogers' 
         WHEN ds.franchise_tp = 'F' 
             THEN 'Fido' 
         WHEN ds.franchise_tp = 'C' 
             THEN 'Chatr' 
         ELSE '?' 
         END FRANCHISE_NAME,
    CASE 
        WHEN ub.message_type ='1' 
            THEN 'Y' 
        WHEN ub.message_type ='0' 
            THEN 'N' 
        ELSE '?' 
        END ROAMING_FLAG,
    CASE 
        WHEN (
                (
                    (TRIM(ub.format_source) IN ('GSM1205','DPSAMA')         -- VoLTE
                    AND TRIM(ub.message_switch_id) IN ('IMM')               -- Roaming format
                    AND ub.message_type = '0'                               -- Domestic
                    )
                    OR
                    (TRIM(ub.format_source) IN ('LD','CLRHS','RTILD','RHPOA','IMM1205')  -- ViLTE
                    AND TRIM(ub.message_switch_id) IN ('TAP3')              -- Roaming format
                    AND ub.message_type = '1'                               -- Roaming
                    )
                )
                AND
                (ub.bl_prsnt_ftr_cd IN ('VAC','UVA','UFA','ZVC','G&P','OUT','NON')
                OR (ub.bl_prsnt_ftr_cd IN ('VCR','VCT','IRI') AND ub.call_type_feature = 'STD')
                )
                AND
                ub.in_route <> 'VoWiFi'
            ) 
            THEN 'VoLTE LD'
        WHEN (
                (
                    (TRIM(ub.format_source) IN ('GSM1205','DPSAMA')         -- Roaming LTE
                    AND TRIM(ub.message_switch_id) IN ('IMM')               -- Roaming format
                    AND ub.message_type = '0'                               -- Domestic
                    )
                    OR
                    (TRIM(ub.format_source) IN ('LD','CLRHS','RTILD','RHPOA','IMM1205')  -- LD LTE
                    AND TRIM(ub.message_switch_id) IN ('TAP3')              -- Roaming format
                    AND ub.message_type = '1'                               -- Roaming
                    )
                )
                AND
                (ub.bl_prsnt_ftr_cd IN ('VRO','VCO','VCR','VRM','PVI','PVO','VCT','VLO','VRI','VON','VGS','VCI','VGP','G&P','OUT','NON') -- ViLTE
                OR (ub.bl_prsnt_ftr_cd IN ('VCR','VCT','IRI') AND ub.call_type_feature = 'VIDEO')
                )
                AND
                ub.in_route <> 'VoWiFi'
            )
            THEN 'ViLTE LD'
        WHEN (
                ub.in_route = 'VoWiFi'
                AND
                ub.call_type_feature = 'STD'
             )
            THEN 'VoWiFi LD'
        WHEN (
                ub.in_route = 'VoWiFi'
                AND
                ub.call_type_feature = 'VIDEO'
             )
            THEN 'ViWiFi LD' 
        ELSE 'Non IMS LD'
        END LD_FLAG,
    CASE 
        WHEN ub.call_action_code = '1' 
            THEN 'MTC' 
        WHEN ub.call_action_code = '2' 
            THEN 'MOC' 
        ELSE ub.call_action_code 
        END CALL_DIRECTION,
    CASE 
        WHEN ub.serve_country_cd IS NULL 
            THEN 'CANADA' 
        WHEN ub.serve_country_cd = 1 
            THEN 'USA' 
        ELSE TRIM(ub.serve_place) 
        END COUNTRY_OF_SUBSCRIBER,
    CASE 
        WHEN (
                (
                    ub.call_action_code = '2'
                    AND
                    (
                    TRIM(sc.country_code) = 'CAN'
                    OR
                    TRIM(ub.mps_ld_feature_code) = 'CAN' 
                    OR 
                    TRIM(ub.mps_ld_feature_code) LIKE '%CAN%'
                    )
                )
                OR (
                    ub.call_action_code = '1'
                    AND 
                    ub.serve_country_cd IS NULL
                )
            )
            THEN 'CANADA'  
        WHEN (
                (
                    ub.call_action_code = '2'
                    AND
                    (
                    TRIM(sc.country_code) = 'USA'
                    OR
                    TRIM(ub.mps_ld_feature_code) = 'US' 
                    OR
                    TRIM(ub.mps_ld_feature_code) LIKE '%US%'
                    )                    
                )
                OR (
                    ub.call_action_code = '1'
                    AND
                    ub.serve_country_cd = 1
                )
            )
            THEN 'USA'             
        ELSE 'INTERNATIONAL'
        END ZONE,
    CASE 
        WHEN TRIM(ub.mps_ld_feature_code) LIKE '%SPNUM%' 
            THEN 'SPECIAL NUMBER' 
        WHEN (ub.call_action_code = '2' 
              AND
              (
                TRIM(sc.country_code) = 'CAN'
                OR
                TRIM(ub.mps_ld_feature_code) = 'CAN' 
                OR 
                TRIM(ub.mps_ld_feature_code) LIKE '%CAN%'
                OR 
                TRIM(ub.mps_ld_feature_code) IS NULL
                AND
                TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'
              )
            )
            THEN NVL(UPPER(TRIM(CONCAT(ub.call_to_city_desc,', ','CANADA'))),'UNKNOWN, CANADA')
        WHEN (ub.call_action_code = '2'                                                                             -- Mobile Originated Call (MOC)
              AND
              (
                TRIM(sc.country_code) = 'USA' -- USA state codes
                OR
                TRIM(ub.mps_ld_feature_code) = 'US'                                         -- USA
                OR 
                TRIM(ub.mps_ld_feature_code) LIKE '%US%'                                    -- USA
                AND
                TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'                             -- Special Number
              )
            )
            THEN 'USA'
        WHEN (ub.call_action_code = '1'                                                     -- Mobile Terminated Call (MTC)
              AND
              ub.serve_country_cd IS NULL                                                   -- CANADA
              AND
              ub.message_type = '0'                                                         -- Domestic
              AND
              TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'                               -- Not a Special Number
            )
            THEN NVL(UPPER(TRIM(CONCAT(ub.serve_place,', ','CANADA'))),'UNKNOWN, CANADA')
        WHEN (ub.call_action_code = '1'                                                     -- Mobile Terminated Call (MTC)
              AND  
              ub.serve_country_cd = 1                                                       -- USA
              AND
              ub.message_type = '1'                                                         -- Roaming
              AND
              TRIM(ub.message_switch_id) = 'TAP3'                                           -- Roaming Format
              AND
              TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'                               -- Not a Special Number
            )
            THEN 'USA'
        ELSE
            NVL(UPPER(TRIM(ub.call_to_city_desc)),'UNKNOWN, INTERNATIONAL')                 -- City/Country where the call was made
        END REFERENCE_DESTINATION,
    COUNT(DISTINCT ub.subscriber_no) AS UNIQUE_SUBSCRIBERS,
    COUNT(0) AS CONNECTED_CALLS,
    CAST(SUM(ub.rounded_units) AS BIGINT) AS ROUNDED_MINUTES,
    CURRENT_DATE() AS LAST_UPDATED_DT,
    DATE_FORMAT(ub.channel_seizure_dt, 'yyyy-MM') AS MONTH
FROM 
    (
        SELECT 
            subscriber_no,
            ban,
            in_route,
            bl_prsnt_ftr_cd,
            call_type_feature,
            toll_type,
            us_stream_type,
            message_type,
            format_source,
            message_switch_id,
            call_action_code,
            serve_country_cd,
            serve_place,
            call_to_state_code,
            call_to_city_desc,
            mps_ld_feature_code,
            rounded_units,
            channel_seizure_dt
        FROM elau_usage.usage_billed 
        WHERE
        us_stream_type = 'V'                             -- Voice
        AND 
        (
            (
                call_action_code = '2'                      -- MOC
                AND TRIM(format_source) IN ('GSM1205','DPSAMA')   -- Non IMS, Do not include TAP3
                AND serve_country_cd IS NULL                -- Canada
                AND toll_type IN ('3','4','5')              -- Long Distance
            )
            OR
            (
                call_action_code = '2'                      -- MOC
                AND TRIM(format_source) IN ('LD','CLRHS','RTILD','RHPOA','IMM1205') -- VoLTE LD
                AND toll_type IN ('3','4','5')              -- Long Distance
            )
            OR
            (
                call_action_code = '1'                      -- MTC
                AND serve_country_cd IS NOT NULL            -- Not Canada
            )
            OR
            (
                bl_prsnt_ftr_cd IN ('WRV', 'WRO')            -- WiFi
                AND in_route = 'VoWiFi'
            )
        )
        AND cycle_run_month = ${hivevar:cycle_run_month}
        AND cycle_run_year = ${hivevar:cycle_run_year}
        AND DATE_FORMAT(channel_seizure_dt, 'yyyy-MM') >= '${hivevar:usage_start_month}'
        AND DATE_FORMAT(channel_seizure_dt, 'yyyy-MM') <= '${hivevar:usage_end_month}'
    ) ub 
    LEFT OUTER JOIN 
    ( 
     SELECT subscriber_no,
            billing_account_number,
            subscriber_status,
            account_type,
            account_sub_type,
            product_type,
            platform,
            franchise_tp,
            deactivation_date  
      FROM app_shared_dim.dim_subscriber              
      WHERE (
             date_format(deactivation_date,'yyyy-MM') >=  '${hivevar:usage_start_month}'
             OR deactivation_date IS NULL
            )
    ) ds                             
    ON (ds.subscriber_no = ub.subscriber_no
        AND ds.billing_account_number = ub.ban)
    LEFT OUTER JOIN
    (
      SELECT description,
             acc_type,
             acc_sub_type
      FROM ela_v21.account_type_gg
    ) at
    ON (at.acc_type = ds.account_type 
        AND at.acc_sub_type = ds.account_sub_type)
    LEFT OUTER JOIN 
    (
       SELECT product_desc,
              product_type
       FROM ela_v21.product_gg
    ) pt
    ON (pt.product_type = ds.product_type)
    LEFT OUTER JOIN
    (
       SELECT state_code,
              country_code
       FROM cdrdm.dim_state_mps
    ) sc
    ON sc.state_code = ub.call_to_state_code
GROUP BY
    NVL(TRIM(at.description),'?'),
    NVL(TRIM(pt.product_desc),'?'),
    NVL(TRIM(ds.platform),'?'),
    CASE 
         WHEN ds.franchise_tp = 'R' 
             THEN 'Rogers' 
         WHEN ds.franchise_tp = 'F' 
             THEN 'Fido' 
         WHEN ds.franchise_tp = 'C' 
             THEN 'Chatr' 
         ELSE '?' 
         END,
    CASE 
        WHEN ub.message_type ='1' 
            THEN 'Y' 
        WHEN ub.message_type ='0' 
            THEN 'N' 
        ELSE '?' 
        END,
    CASE 
        WHEN (
                (
                    (TRIM(ub.format_source) IN ('GSM1205','DPSAMA')         -- VoLTE
                    AND TRIM(ub.message_switch_id) IN ('IMM')               -- Roaming format
                    AND ub.message_type = '0'                               -- Domestic
                    )
                    OR
                    (TRIM(ub.format_source) IN ('LD','CLRHS','RTILD','RHPOA','IMM1205')  -- ViLTE
                    AND TRIM(ub.message_switch_id) IN ('TAP3')              -- Roaming format
                    AND ub.message_type = '1'                               -- Roaming
                    )
                )
                AND
                (ub.bl_prsnt_ftr_cd IN ('VAC','UVA','UFA','ZVC','G&P','OUT','NON')
                OR (ub.bl_prsnt_ftr_cd IN ('VCR','VCT','IRI') AND ub.call_type_feature = 'STD')
                )
                AND
                ub.in_route <> 'VoWiFi'
            ) 
            THEN 'VoLTE LD'
        WHEN (
                (
                    (TRIM(ub.format_source) IN ('GSM1205','DPSAMA')         -- Roaming LTE
                    AND TRIM(ub.message_switch_id) IN ('IMM')               -- Roaming format
                    AND ub.message_type = '0'                               -- Domestic
                    )
                    OR
                    (TRIM(ub.format_source) IN ('LD','CLRHS','RTILD','RHPOA','IMM1205')  -- LD LTE
                    AND TRIM(ub.message_switch_id) IN ('TAP3')              -- Roaming format
                    AND ub.message_type = '1'                               -- Roaming
                    )
                )
                AND
                (ub.bl_prsnt_ftr_cd IN ('VRO','VCO','VCR','VRM','PVI','PVO','VCT','VLO','VRI','VON','VGS','VCI','VGP','G&P','OUT','NON') -- ViLTE
                OR (ub.bl_prsnt_ftr_cd IN ('VCR','VCT','IRI') AND ub.call_type_feature = 'VIDEO')
                )
                AND
                ub.in_route <> 'VoWiFi'
            )
            THEN 'ViLTE LD'
        WHEN (
                ub.in_route = 'VoWiFi'
                AND
                ub.call_type_feature = 'STD'
             )
            THEN 'VoWiFi LD'
        WHEN (
                ub.in_route = 'VoWiFi'
                AND
                ub.call_type_feature = 'VIDEO'
             )
            THEN 'ViWiFi LD' 
        ELSE 'Non IMS LD'
        END,
    CASE 
        WHEN ub.call_action_code = '1' 
            THEN 'MTC' 
        WHEN ub.call_action_code = '2' 
            THEN 'MOC' 
        ELSE ub.call_action_code 
        END,
    CASE 
        WHEN ub.serve_country_cd IS NULL 
            THEN 'CANADA' 
        WHEN ub.serve_country_cd = 1 
            THEN 'USA' 
        ELSE TRIM(ub.serve_place) 
        END,
    CASE 
        WHEN (
                (
                    ub.call_action_code = '2'
                    AND
                    (
                    TRIM(sc.country_code) = 'CAN'
                    OR
                    TRIM(ub.mps_ld_feature_code) = 'CAN' 
                    OR 
                    TRIM(ub.mps_ld_feature_code) LIKE '%CAN%'
                    )
                )
                OR (
                    ub.call_action_code = '1'
                    AND 
                    ub.serve_country_cd IS NULL
                )
            )
            THEN 'CANADA'  
        WHEN (
                (
                    ub.call_action_code = '2'
                    AND
                    (
                    TRIM(sc.country_code) = 'USA'
                    OR
                    TRIM(ub.mps_ld_feature_code) = 'US' 
                    OR
                    TRIM(ub.mps_ld_feature_code) LIKE '%US%'
                    )                    
                )
                OR (
                    ub.call_action_code = '1'
                    AND
                    ub.serve_country_cd = 1
                )
            )
            THEN 'USA'             
        ELSE 'INTERNATIONAL'
        END,
    CASE 
        WHEN TRIM(ub.mps_ld_feature_code) LIKE '%SPNUM%' 
            THEN 'SPECIAL NUMBER' 
        WHEN (ub.call_action_code = '2' 
              AND
              (
                TRIM(sc.country_code) = 'CAN'
                OR
                TRIM(ub.mps_ld_feature_code) = 'CAN' 
                OR 
                TRIM(ub.mps_ld_feature_code) LIKE '%CAN%'
                OR 
                TRIM(ub.mps_ld_feature_code) IS NULL
                AND
                TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'
              )
            )
            THEN NVL(UPPER(TRIM(CONCAT(ub.call_to_city_desc,', ','CANADA'))),'UNKNOWN, CANADA')
        WHEN (ub.call_action_code = '2'                                                                             -- Mobile Originated Call (MOC)
              AND
              (
                TRIM(sc.country_code) = 'USA' -- USA state codes
                OR
                TRIM(ub.mps_ld_feature_code) = 'US'                                         -- USA
                OR 
                TRIM(ub.mps_ld_feature_code) LIKE '%US%'                                    -- USA
                AND
                TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'                             -- Special Number
              )
            )
            THEN 'USA'
        WHEN (ub.call_action_code = '1'                                                     -- Mobile Terminated Call (MTC)
              AND
              ub.serve_country_cd IS NULL                                                   -- CANADA
              AND
              ub.message_type = '0'                                                         -- Domestic
              AND
              TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'                               -- Not a Special Number
            )
            THEN NVL(UPPER(TRIM(CONCAT(ub.serve_place,', ','CANADA'))),'UNKNOWN, CANADA')
        WHEN (ub.call_action_code = '1'                                                     -- Mobile Terminated Call (MTC)
              AND  
              ub.serve_country_cd = 1                                                       -- USA
              AND
              ub.message_type = '1'                                                         -- Roaming
              AND
              TRIM(ub.message_switch_id) = 'TAP3'                                           -- Roaming Format
              AND
              TRIM(ub.mps_ld_feature_code) NOT LIKE '%SPNUM%'                               -- Not a Special Number
            )
            THEN 'USA'
        ELSE
            NVL(UPPER(TRIM(ub.call_to_city_desc)),'UNKNOWN, INTERNATIONAL')                 -- City/Country where the call was made
        END,
    CURRENT_DATE(),
    DATE_FORMAT(ub.channel_seizure_dt, 'yyyy-MM');

!echo "Loaded into cdrdm.fact_volte_ldrpts_cdr_temp table";