--[Version History]
--0.1 - danchang - 4/06/2016 - initial version
--0.2 - danchang - 4/25/2016 - reverted EXT_ELA_V21 schemas back to ELA_V21
--0.3 - danchang - 4/27/2016 - added drop statement
--0.4 - saseenthar - 6/10/2016 - Updated to accomodate source system changes as part of UCC Phase 2 requirement

SET hive.variable.substitute=true;
SET hive.auto.convert.join=true;
SET hive.default.fileformat=textfile;
SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.optimize.index.filter=false;
SET hive.optimize.constant.propagation=false;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.tez.auto.reducer.parallelism=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.exec.orc.default.buffer.size=32768;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.vectorized.execution.reduce.groupby.enabled=false;


TRUNCATE TABLE cdrdm.DIM_UCC_Sub_Info;

INSERT INTO TABLE cdrdm.DIM_UCC_Sub_Info 
SELECT BAN, Subscriber_no, SOC, FEATURE_CODE, cast(FTR_EFFECTIVE_DATE AS string) AS EFFECTIVE_DATE, cast(FTR_EXPIRATION_DATE AS string) AS EXPIRATION_DATE, Product_type, '' AS INIT_ACTIVATION_DATE, '' AS SUB_STATUS, '' AS ALS_IND, '' AS Netwrk,  cast(SYS_CREATION_DATE AS string), cast(SYS_UPDATE_DATE AS string), 'FTR' AS Tab_Type, '' AS sub_exp1, '' AS sub_exp2, FROM_UNIXTIME(UNIX_TIMESTAMP()) AS insert_ts
--[DanChang] added null values for 2 missing columns (sub_exp1/sub_exp2) and new timestamp column (insert_ts)
FROM ELA_V21.Service_feature  X
--[Saseenthar] Phase 2 changes starts
--WHERE X.FEATURE_CODE IN (SELECT Y.FEATURE_CODE FROM ELA_V21.feature_GG Y WHERE Y.switch_code IN ('UCCAUC','UCCENT','UCCHUN','UCCUSR'))
--WHERE X.FEATURE_CODE IN (Select Y.FEATURE_CODE  from ELA_V21.feature_gg Y where Y.switch_code in (Select Z.SW_Code from cdrdm.dim_Sw_codes Z where Z.pick = --'Y'))
LEFT SEMI JOIN
(Select Y.FEATURE_CODE  from ELA_V21.feature_gg Y where Y.switch_code in (Select Z.SW_Code from cdrdm.dim_Sw_codes Z where Z.pick = 'Y')) GG
ON
X.FEATURE_CODE = GG.FEATURE_CODE
--[Saseenthar] Phase 2 changes ends
--[DanChang] feature table is now feature_gg
UNION ALL

SELECT BAN, Subscriber_no, SOC, FEATURE_CODE, cast(FTR_EFFECTIVE_DATE AS string) AS EFFECTIVE_DATE, cast(FTR_EXPIRATION_DATE AS string) AS EXPIRATION_DATE, Product_type, '' AS INIT_ACTIVATION_DATE, '' AS SUB_STATUS, '' AS ALS_IND, ''AS Netwrk,  cast(SYS_CREATION_DATE AS string), cast(SYS_UPDATE_DATE AS string),'FTR_HIST' AS Tab_Type, '' AS sub_exp1, '' AS sub_exp2, from_unixtime(unix_timestamp()) AS insert_ts 
--[DanChang] added null values for 2 missing columns (sub_exp1/sub_exp2) and new timestamp column (insert_ts)
FROM ELA_V21.srv_ftr_history  X
--[Saseenthar] Phase 2 changes starts
--WHERE X.FEATURE_CODE IN (SELECT Y.FEATURE_CODE FROM ELA_V21.feature_GG Y WHERE Y.switch_code IN ('UCCAUC','UCCENT','UCCHUN','UCCUSR'))
--WHERE X.FEATURE_CODE IN (Select Y.FEATURE_CODE  from ELA_V21.feature_gg Y where Y.switch_code in (Select Z.SW_Code from cdrdm.dim_Sw_codes Z where Z.pick = --'Y'))
LEFT SEMI JOIN
(Select Y.FEATURE_CODE  from ELA_V21.feature_gg Y where Y.switch_code in (Select Z.SW_Code from cdrdm.dim_Sw_codes Z where Z.pick = 'Y')) GG
ON
X.FEATURE_CODE = GG.FEATURE_CODE
--[Saseenthar] Phase 2 changes ends
--[DanChang] feature table is now feature_gg

UNION ALL

--[DanChang] separated this select statement into 2 separate statements due to Hive limitation where subquery predicates must appear as top level conjuncts.
--Select * from table where A or B was translated into:
--Select * from table where A
--UNION ALL
--Select * from table where (not A) and B
SELECT CUSTOMER_BAN AS BAN, SUBSCRIBER_NO, '' AS SOC, '' AS FEATURE_CODE, EFFECTIVE_DATE, '' AS EXPIRATION_DATE, Product_type, INIT_ACTIVATION_DATE, SUB_STATUS, ALS_IND, Netwrk, SYS_CREATION_DATE, SYS_UPDATE_DATE,'SUBS' AS Tab_Type, '' AS sub_exp1, '' AS sub_exp2, from_unixtime(unix_timestamp()) AS insert_ts
--[DanChang] added null values for 2 missing columns (sub_exp1/sub_exp2) and new timestamp column (insert_ts)
FROM ELA_V21.SUBSCRIBER X
WHERE X.ALS_IND IN ('P','H','V','U')

UNION ALL
--[DanChang] this is the second part of the select statement
SELECT CUSTOMER_BAN AS BAN, SUBSCRIBER_NO, '' AS SOC, '' AS FEATURE_CODE, EFFECTIVE_DATE, '' AS EXPIRATION_DATE, Product_type, INIT_ACTIVATION_DATE, SUB_STATUS, ALS_IND, Netwrk, SYS_CREATION_DATE, SYS_UPDATE_DATE,'SUBS' AS Tab_Type, '' AS sub_exp1, '' AS sub_exp2, from_unixtime(unix_timestamp()) AS insert_ts
--[DanChang] added null values for 2 missing columns (sub_exp1/sub_exp2) and new timestamp column (insert_ts)
FROM ELA_V21.SUBSCRIBER X
WHERE X.ALS_IND NOT IN ('P','H','V','U')
AND EXISTS (SELECT 1 FROM 
            (
                --[DanChang] modified/added aliases to promote readability
                SELECT subSF.BAN, subSF.Subscriber_no, subSF.FTR_EFFECTIVE_DATE, subSF.FTR_EXPIRATION_DATE 
                FROM ELA_V21.Service_feature subSF
                --[Saseenthar] Phase 2 changes starts
                --WHERE subSF.FEATURE_CODE IN (SELECT GG1.FEATURE_CODE FROM ELA_V21.feature_GG GG1 WHERE GG1.switch_code IN ('UCCAUC','UCCENT','UCCHUN','UCCUSR'))
                --WHERE subSF.FEATURE_CODE IN (SELECT GG1.FEATURE_CODE FROM ELA_V21.feature_gg GG1 WHERE GG1.switch_code IN (Select SW1.SW_Code from --cdrdm.dim_Sw_codes SW1 where SW1.pick = 'Y'))
                LEFT SEMI JOIN 
                (SELECT GG1.FEATURE_CODE FROM ELA_V21.feature_gg GG1 WHERE GG1.switch_code IN (Select SW1.SW_Code from cdrdm.dim_Sw_codes SW1 where SW1.pick = 'Y')) GG3
                ON
                subSF.FEATURE_CODE = GG3.FEATURE_CODE
                --[Saseenthar] Phase 2 changes ends
                UNION ALL
                SELECT subSFH.BAN, subSFH.Subscriber_no, subSFH.FTR_EFFECTIVE_DATE, subSFH.FTR_EXPIRATION_DATE 
                FROM ELA_V21.srv_ftr_history subSFH
                --[Saseenthar] Phase 2 changes starts				
                --WHERE subSFH.FEATURE_CODE IN (SELECT GG2.FEATURE_CODE FROM ELA_V21.feature_GG GG2 WHERE GG2.switch_code IN ('UCCAUC','UCCENT','UCCHUN','UCCUSR'))
                --WHERE subSFH.FEATURE_CODE IN (SELECT GG2.FEATURE_CODE FROM ELA_V21.feature_gg GG2 WHERE GG2.switch_code IN (Select SW2.SW_Code from --cdrdm.dim_Sw_codes SW2 where SW2.pick = 'Y'))
                LEFT SEMI JOIN
                (SELECT GG2.FEATURE_CODE FROM ELA_V21.feature_gg GG2 WHERE GG2.switch_code IN (Select SW2.SW_Code from cdrdm.dim_Sw_codes SW2 where SW2.pick = 'Y')) GG4
                ON
                subSFH.FEATURE_CODE = GG4.FEATURE_CODE
                --[Saseenthar] Phase 2 changes ends				
                ) Y WHERE X.SUBSCRIBER_NO = Y.SUBSCRIBER_NO AND X.customer_ban = y.ban 
                -- /* and X.EFFECTIVE_DATE >= Y.FTR_EFFECTIVE_DATE */ --to be finalized after looking at sample data and consulation with source 
            )

UNION ALL

--[DanChang] separated this select statement into 2 separate statements due to Hive limitation where subquery predicates must appear as top level conjuncts.
SELECT CUSTOMER_BAN AS BAN, SUBSCRIBER_NO, '' AS SOC, '' AS FEATURE_CODE, cast(EFFECTIVE_DATE AS string), cast(EXPIRATION_DATE as string), Product_type, cast(INIT_ACTIVATION_DATE AS string), SUB_STATUS, ALS_IND, Netwrk, cast(SYS_CREATION_DATE AS string), cast(SYS_UPDATE_DATE AS string), 'SUBS_HIST' AS Tab_Type, '' AS sub_exp1, '' AS sub_exp2, from_unixtime(unix_timestamp()) AS insert_ts
--[DanChang] added null values for 2 missing columns (sub_exp1/sub_exp2) and new timestamp column (insert_ts)
FROM ELA_V21.Subscriber_history X
WHERE X.ALS_IND IN ('P','H','V','U')

UNION ALL
--[DanChang] this is the second part of the select statement
SELECT CUSTOMER_BAN AS BAN, SUBSCRIBER_NO, '' AS SOC, '' AS FEATURE_CODE, cast(EFFECTIVE_DATE as string), cast(EXPIRATION_DATE as string), Product_type, cast(INIT_ACTIVATION_DATE AS string), SUB_STATUS, ALS_IND, Netwrk, cast(SYS_CREATION_DATE AS string), cast(SYS_UPDATE_DATE AS string), 'SUBS_HIST' AS Tab_Type, '' AS sub_exp1, '' AS sub_exp2, from_unixtime(unix_timestamp()) AS insert_ts
--[DanChang] added null values for 2 missing columns (sub_exp1/sub_exp2) and new timestamp column (insert_ts)
FROM ELA_V21.Subscriber_history X
WHERE X.ALS_IND NOT IN ('P','H','V','U') 
AND EXISTS (SELECT 1 FROM
            (
                --[DanChang] modified/added aliases to promote readability
                SELECT SF.BAN, SF.Subscriber_no, SF.FTR_EFFECTIVE_DATE, SF.FTR_EXPIRATION_DATE
                FROM ELA_V21.Service_feature  SF
                --[Saseenthar] Phase 2 changes starts
                --WHERE SF.FEATURE_CODE IN (SELECT FGG1.FEATURE_CODE FROM ELA_V21.feature_GG FGG1 WHERE FGG1.switch_code IN ('UCCAUC','UCCENT','UCCHUN','UCCUSR'))
                --WHERE SF.FEATURE_CODE IN (SELECT FGG1.FEATURE_CODE FROM ELA_V21.feature_gg FGG1 WHERE FGG1.switch_code IN (Select FSW1.SW_Code from --cdrdm.dim_Sw_codes FSW1 where FSW1.pick = 'Y'))
                LEFT SEMI JOIN
                (SELECT FGG1.FEATURE_CODE FROM ELA_V21.feature_gg FGG1 WHERE FGG1.switch_code IN (Select FSW1.SW_Code from cdrdm.dim_Sw_codes FSW1 where FSW1.pick = 'Y')) FGG3
                ON
                SF.FEATURE_CODE = FGG3.FEATURE_CODE
                --[Saseenthar] Phase 2 changes ends                
                UNION ALL
                SELECT SFH.BAN, SFH.Subscriber_no, SFH.FTR_EFFECTIVE_DATE, SFH.FTR_EXPIRATION_DATE 
                FROM ELA_V21.srv_ftr_history  SFH
                --[Saseenthar] Phase 2 changes ends
                --WHERE SFH.FEATURE_CODE IN (SELECT FGG2.FEATURE_CODE FROM ELA_V21.feature_GG FGG2 WHERE FGG2.switch_code IN ('UCCAUC','UCCENT','UCCHUN','UCCUSR'))
                --WHERE SFH.FEATURE_CODE IN (SELECT FGG2.FEATURE_CODE FROM ELA_V21.feature_gg FGG2 WHERE FGG2.switch_code IN (Select FSW2.SW_Code from --cdrdm.dim_Sw_codes FSW2 where FSW2.pick = 'Y'))
                LEFT SEMI JOIN
                (SELECT FGG2.FEATURE_CODE FROM ELA_V21.feature_gg FGG2 WHERE FGG2.switch_code IN (Select FSW2.SW_Code from cdrdm.dim_Sw_codes FSW2 where FSW2.pick = 'Y')) FGG4
                ON
                SFH.FEATURE_CODE = FGG4.FEATURE_CODE
                --[Saseenthar] Phase 2 changes ends
                ) Y WHERE X.SUBSCRIBER_NO = Y.SUBSCRIBER_NO AND X.customer_ban = y.ban 
             -- /* and X.EFFECTIVE_DATE >= Y.FTR_EFFECTIVE_DATE  and X.EXPIRATION_DATE <= Y.FTR_EXPIRATION_DATE */ --to be finalized after looking at sample data and consulation with source 
            )
;
