
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

SET WHERE_clause;
-- WHERE_clause= JOIN (SELECT tmp_file_name FROM ext_cdrdm.cdr_processing WHERE tmp_source='siumsmsraw' AND tmp_destination='FACT_SMS_CDR') tmp 
--               ON a.file_name=tmp.tmp_file_name WHERE a.file_date IN ('2016-09-13','2016-09-14')



DROP TABLE IF EXISTS ext_cdrdm.subscriber_ocn;
DROP TABLE IF EXISTS ext_cdrdm.siumsmsraw_temp1;
DROP TABLE IF EXISTS ext_cdrdm.siumsmsraw_temp2;

------------------------------------------------------------------------------
-- Comment from EXPR_OCN_Fmt expression from m_BIDW_CDR_SMS_Stg_FF to prepare OCN field which needs 
-- DIM_SUBSCRIBER & DIM_SERVICE_PROVIDER tables

-- Remove leading '1' from source field to cleanse the source field.
-- Check to see if the cleansed source field (Other_MSISDN_OUT) exists as a subscriber_no in dim_subscriber.

-- If it exists then use OCN from dim_service_provider where company name = 'Rogers AT&T Wireless' 

-- If it doesnt exist then join the first 3 digits of the source to Dim_interaction_source.NPA 
-- and digits 4-6 of the source to dim_interaction_source.NXX to find the OCN.  If there is no match, 
-- then if the length of the source is 10 then insert 'XXXX' if the lenght is not 10 then insert 'N/A'
------------------------------------------------------------------------------

-- below Cartesian JOIN with only 1 value from DIM_SERVICE_PROVIDER table 
-- will prepare result set to be used in 2nd pass below to prepare OCN field

CREATE TABLE IF NOT EXISTS ext_cdrdm.subscriber_ocn AS
WITH dsp AS (SELECT ocn FROM cdrdm.dim_service_provider WHERE company_name='Rogers AT&T Wireless') -- will return only 1 OCN value
SELECT /*+ MAPJOIN(dsp) */ sbcr.subscriber_no, dsp.ocn 
FROM (SELECT subscriber_no FROM cdrdm.dim_subscriber GROUP BY subscriber_no) sbcr, dsp
;

------------------------------------------------------------------------------
-- Informatica mapping #1 - m_BIDW_CDR_SMS_Stg_FF
CREATE TABLE IF NOT EXISTS ext_cdrdm.siumsmsraw_temp1 AS
SELECT
-- UPDATED
-- substr(trim(CHARGED_MSISDN),2,length(CHARGED_MSISDN)) as subscriber_no,
CAST(IF(LENGTH(LTRIM(SUBSTR(CHARGED_MSISDN,1,20))) <> 11, LTRIM(SUBSTR(CHARGED_MSISDN,1,20)), SUBSTR(LTRIM(SUBSTR(CHARGED_MSISDN,1,20)),2,20)) AS BIGINT) AS subscriber_no,

CAST(EVENTRECORDID AS SMALLINT) as event_record_id,
-- UPDATED
CAST(SPID AS SMALLINT) as spid, -- converted to INT
CAST(if(ORIGMSC is not null,ORIGMSC,'-1') AS VARCHAR(30)) as orig_msc, -- STRING --if (ORIGMSC is not null,ORIGMSC,0) as orig_msc,

-- UPDATED
-- if (substr(trim(OTHER_MSISDN),1,1) <> 1, substr(trim(OTHER_MSISDN),1,20), substr(substr(trim(OTHER_MSISDN),1,20),2,length(substr(trim(OTHER_MSISDN),1,20)))) as other_msisdn,
CAST(IF( LENGTH(TRIM(SUBSTR(OTHER_MSISDN,1,20))) <> 11, TRIM(SUBSTR(OTHER_MSISDN,1,20)), SUBSTR(TRIM(SUBSTR(OTHER_MSISDN,1,20)),2,20)) AS BIGINT) AS OTHER_MSISDN,
-- store above in INT data type - not STRING

-- New TEMP field to be used for OCN later
CAST( REGEXP_REPLACE( if(length(REGEXP_REPLACE(RTRIM(SUBSTR( OTHER_MSISDN, 1, 20 )), '^1*', ''))>18,substr(REGEXP_REPLACE(RTRIM(SUBSTR( OTHER_MSISDN, 1, 20 )), '^1*', ''),1,18),REGEXP_REPLACE(RTRIM(SUBSTR( OTHER_MSISDN, 1, 20 )), '^1*', '')), 'e', '') AS BIGINT) AS ocn_Other_MSISDN,

-- UPDATED
CAST(if(ORIG_MSC_ZONE_IND is not null,ORIG_MSC_ZONE_IND,-1) AS SMALLINT) as orig_msc_zone_indicator,  -- if (ORIG_MSC_ZONE_IND is not null,ORIG_MSC_ZONE_IND,0)
CAST(if(RATE is not NULL,RATE,0) AS SMALLINT) as rate,  --INT
CAST(CHARGEDPARTYSTRING AS CHAR(1)) as charged_party,
CAST(ACTION AS SMALLINT) as action,
--------------- SIUMSMSRAW Table does not populate NULL value properly for RULE, TRANSACTION_ID, CHARGE_RESULT, BALANCE, RATE_PLAN 
-- CAST(if(RULE is not null,RULE,0) AS INT) as rule,
CAST(IF(LENGTH(RULE)<>0,RULE,0) AS INT) as rule,
-- CAST(if(TRANSACTIONID is not null,TRANSACTIONID,0) AS INT) as transaction_id,
CAST(IF(LENGTH(TRANSACTIONID)<>0,TRANSACTIONID,0) AS INT) as transaction_id,
-- CAST(if(CHARGERESULT is not null,CHARGERESULT,0) AS SMALLINT) as charge_result,
CAST(IF(LENGTH(CHARGERESULT)<>0,CHARGERESULT,0) AS SMALLINT) as charge_result,
-- CAST(if(BALANCE is not null,BALANCE,0) AS INT) as balance,
CAST(IF(LENGTH(BALANCE)<>0,BALANCE,0) AS INT) as balance,
-- CAST(if(RATEPLAN is not null,RATEPLAN,-1) AS SMALLINT) as rate_plan,
CAST(IF(LENGTH(RATEPLAN)<>0,RATEPLAN,-1) AS SMALLINT) as rate_plan,
CAST(IF(LENGTH(ON_NET)<>0,ON_NET,NULL) AS CHAR(1)) as on_net,
------------------------------------------------------------
CAST(ORIGINATING_SERVICEGRADE AS SMALLINT) as originating_service_grade,
CAST(TERMINATING_SERVICEGRADE AS SMALLINT) as terminating_service_grade,
CAST(SEQUENCE_NUMBER AS INT) as sequence_number,  -- INT
concat(substr(local_subscriber_date,1,4),'-',substr(local_subscriber_date,6,2),'-',substr(local_subscriber_date,9,2),' ',local_subscriber_time) as local_subscriber_timestamp, -- STRING
-- UPDATED
CAST( (if(IMSI is not null,IMSI,-1)) AS BIGINT) as imsi, -- if(IMSI is not null,IMSI,0)
CAST(BAN AS BIGINT) as ban,  -- UPDATED with type conversion
CAST(if(LANGUAGE is not null,LANGUAGE,' ') AS CHAR(1)) as language,
CAST(BILL_CYCLE_DATE AS SMALLINT) as bill_cycle_date,
CAST(if(ORIGINATING_SUBS_LOCATION is not null,ORIGINATING_SUBS_LOCATION,' ') AS CHAR(2)) as orignating_subscriber_loc, -- originating_subscriber_location,
CAST(if(TERMINATING_SUBS_LOCATION is not null,TERMINATING_SUBS_LOCATION,' ') AS CHAR(2)) as terminating_subscriber_loc, -- terminating_subscriber_location

-- CAST(if(RATING_RULE_DESC_1 IS NOT NULL,trim(RATING_RULE_DESC_1),' ') AS VARCHAR(30)) as rating_rule_description_1,
CAST(IF(LENGTH(RATING_RULE_DESC_1)<>0,trim(RATING_RULE_DESC_1),' ') AS VARCHAR(30)) as rating_rule_description_1,

-- UPDATED
-- if(trim(RATING_RULE_DESC_2) = 0,RATING_RULE_DESC_1,'') as rating_rule_description_2,
-- if(LENGTH(rtrim(RATING_RULE_DESC_2)) = 0,if(RATING_RULE_DESC_1 IS NOT NULL,trim(RATING_RULE_DESC_1),' '),'') as rating_rule_description_2,
-- CAST(if(LENGTH(RTRIM(IF(RATING_RULE_DESC_2 IS NOT NULL, RATING_RULE_DESC_2, ' '))) = 0,if(RATING_RULE_DESC_1 IS NOT NULL,trim(RATING_RULE_DESC_1),' '),' ') AS VARCHAR(30)) as rating_rule_description_2,
CAST(if(LENGTH(RTRIM(IF(RATING_RULE_DESC_2 IS NOT NULL, RATING_RULE_DESC_2, ' '))) = 0,if(LENGTH(RATING_RULE_DESC_1)<>0,trim(RATING_RULE_DESC_1),' '),' ') AS VARCHAR(30)) as rating_rule_description_2,

CAST(if(ORIGINATION_EQTYPE is not null,ORIGINATION_EQTYPE,' ') AS CHAR(1)) as origination_eq_type,
CAST(if(DESTINATION_EQTYPE is not null,DESTINATION_EQTYPE,' ') AS CHAR(1)) as destination_eq_type,
CONCAT(substr(sms_date,1,4),'-',substr(sms_date,6,2),'-',substr(sms_date,9,2),' ',sms_time) as date_1, -- check SMS_TIME value / 'YYYY-MM-DD HH24:MI:SS'

-- preparing below two columns to perform JOIN with DIM_INTERACTION_SOURCE tablel in next pass
cast(substr(trim(OTHER_MSISDN),1,3) as CHAR(3)) AS NPA_var,
cast(substr(trim(OTHER_MSISDN),4,3) as CHAR(3)) AS NXX_var,

-- UPDATED
-- if(CHARGEDPARTYSTRING = 'O' and length(trim(RATING_RULE_DESC_1)) >= 10, NULL, if(CHARGEDPARTYSTRING = 'O' and length(trim(RATING_RULE_DESC_1)) < 10, RATING_RULE_DESC_1, if(length(trim(other_msisdn)) >= 10, NULL, substr(trim(OTHER_MSISDN),1,9)))) as other_msisdn_sc,

-- CAST(if(SWITCH_NAME='FPLXRAW' AND CHARGEDPARTYSTRING = 'O' AND length(trim(rating_rule_description_1)) >= 10, NULL, 
--  if(SWITCH_NAME='FPLXRAW' AND CHARGEDPARTYSTRING = 'O' and length(trim(rating_rule_description_1)) < 10, rating_rule_description_1, 
--    if(length(trim(other_msisdn)) >= 10, NULL, substr(OTHER_MSISDN,1,9)))) AS VARCHAR(10)) as other_msisdn_sc,


-- UPDATE due to replace SWITCH_NAME field wiht FILE_NAME...

-- CAST(if(SWITCH_NAME='FPLXRAW' AND CHARGEDPARTYSTRING = 'O' AND LENGTH(IF(RATING_RULE_DESC_1 IS NOT NULL,trim(RATING_RULE_DESC_1),' ')) >= 10, NULL, 
--      if(SWITCH_NAME='FPLXRAW' AND CHARGEDPARTYSTRING = 'O' and LENGTH(IF(RATING_RULE_DESC_1 IS NOT NULL,trim(RATING_RULE_DESC_1),' ')) < 10, (IF(RATING_RULE_DESC_1 IS NOT NULL,trim(RATING_RULE_DESC_1),' ')), 
--       if(length(trim(other_msisdn)) >= 10, NULL, substr(OTHER_MSISDN,1,9)))) AS VARCHAR(10)) as other_msisdn_sc,

-- file_name >>> SIUM_FMMSRAW_ID038954_T20151201000000_R0011418.DAT
-- SUBSTR(file_name,INSTR(file_name,'_')+1,LOCATE('_',file_name,INSTR(file_name,'_')+1 )-INSTR(file_name,'_')-1) >>> FMMSRAW

CAST(IF(SUBSTR(file_name,INSTR(file_name,'_')+1,LOCATE('_',file_name,INSTR(file_name,'_')+1 )-INSTR(file_name,'_')-1)='FPLXRAW' 
        AND CHARGEDPARTYSTRING = 'O' AND LENGTH(IF(LENGTH(RATING_RULE_DESC_1)<>0,trim(RATING_RULE_DESC_1),' ')) >= 10, 
        NULL, 
        IF(SUBSTR(file_name,INSTR(file_name,'_')+1,LOCATE('_',file_name,INSTR(file_name,'_')+1 )-INSTR(file_name,'_')-1)='FPLXRAW' 
           AND CHARGEDPARTYSTRING = 'O' and LENGTH(IF(LENGTH(RATING_RULE_DESC_1)<>0,trim(RATING_RULE_DESC_1),' ')) < 10, 
           (IF(LENGTH(RATING_RULE_DESC_1)<>0,trim(RATING_RULE_DESC_1),' ')), 
            IF(length(trim(other_msisdn)) >= 10, NULL, substr(OTHER_MSISDN,1,9)))) AS VARCHAR(10)) as other_msisdn_sc,

---- Raw Data File fields
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,   
switch_name,
num_records  
FROM cdrdm.siumsmsraw a 
${hiveconf:WHERE_clause};

------------------------------------------------------------------------------
-- Mapping #2 - m_BIDW_CDR_Ins_SMS_HP_Stg 
-- OCN field will be calculated here by doing 2 JOINS with - ext_cdrdm.subscriber_ocn & cdrdm.dim_interaction_source
CREATE TABLE IF NOT EXISTS ext_cdrdm.siumsmsraw_temp2 AS
SELECT
tmp.subscriber_no,
event_record_id,
spid,
orig_msc,
other_msisdn,
orig_msc_zone_indicator,
rate,
charged_party,
action,
rule,
transaction_id,
charge_result,
balance,
rate_plan,
on_net,
originating_service_grade,
terminating_service_grade,
sequence_number,
local_subscriber_timestamp,
imsi,
ban,
language,
bill_cycle_date,
orignating_subscriber_loc, -- originating_subscriber_location,
terminating_subscriber_loc, --terminating_subscriber_location,
rating_rule_description_1,
rating_rule_description_2,
origination_eq_type,
destination_eq_type,
date_1,

--- calculation for OCN field ---
-- Comment this NULL statement when DIM_SUBSCRIBER & DIM_SERVICE_PROVIDER tables are available...
-- CAST(NULL AS CHAR(4)) AS ocn,

-- Uncomment below OCN calculation when DIM_SUBSCRIBER & DIM_SERVICE_PROVIDER tables are available...
CAST(IF(LENGTH(TRIM(CAST(tmp.ocn_Other_MSISDN AS STRING)))=10, 
    IF(sbocn.subscriber_no IS NOT NULL, sbocn.ocn, IF(iact.ocn IS NOT NULL, iact.ocn, 'XXXX')),
    IF(iact.ocn IS NOT NULL, iact.ocn, 'N/A')
   ) AS CHAR(4))AS ocn,
-------------- OCN field ends ---------------------

other_msisdn_sc,
-- Fields for Workflow #2 end here

-- Preparing fields for Workflow #3
if(LENGTH(trim(orig_msc))<12,0,12) as CNL,
CAST(UPPER(SUBSTR(trim(orig_msc),1,1)) AS CHAR(1)) AS TR_Type_OMSC,
if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL) as BNum,
if(tmp.subscriber_no<1000000000,1,if(tmp.subscriber_no>999999999,0,NULL)) as sbscrbr_no_lessthan_1G,
if(trim(CAST(other_msisdn AS STRING))='410002',1,if(trim(CAST(other_msisdn AS STRING))<>'410002',0,NULL)) as other_msisdn_equal_410002,
if((LENGTH(trim(rating_rule_description_1))<10 OR LENGTH(trim(rating_rule_description_2))<10),1,if((LENGTH(trim(rating_rule_description_1))>9 OR LENGTH(trim(rating_rule_description_2))>9),0,NULL)) as rtng_rl_dscrptn_1_2_shorterthan_10chr,
-- Expression: exp_GATHER_SOURCE_FACT_SMS_CDR_22 ends here

-- UPDATED
-- if(find_in_set(UPPER(SUBSTR(trim(orig_msc),1,1)),'A,C,D,V') > 0,SUBSTR(trim(orig_msc),2,6),NULL) as TR_OMSC_PLMN, --refering to TR_Type_OMSC
CAST(IF(find_in_set(UPPER(SUBSTR(trim(orig_msc),1,1)),'A,C,D,V') > 0, IF(SUBSTR(TRIM(orig_msc),2,6) = '000000', NULL, IF(SUBSTR(TRIM(orig_msc),2,3) <> '000' AND SUBSTR(TRIM(orig_msc),5,3) = '000',SUBSTR(TRIM(orig_msc),2,3),SUBSTR(TRIM(orig_msc),2,6))),NULL) AS VARCHAR(7)) AS TR_OMSC_PLMN,

CAST(IF(find_in_set(UPPER(SUBSTR(trim(orig_msc),1,1)),'C,D,V') > 0,UPPER(SUBSTR(trim(orig_msc),8,4)),NULL) AS VARCHAR(5)) as TR_LAC_TAC, --refering to TR_Type_OMSC

-- UPDATED
-- if(UPPER(SUBSTR(trim(orig_msc),1,1))='D',UPPER(SUBSTR(UPPER(trim(SUBSTR(orig_msc,1,1))),12)),if(UPPER(trim(SUBSTR(orig_msc,1,1)))='C',UPPER(SUBSTR(orig_msc,12,(LENGTH(orig_msc)-(if(LENGTH(trim(orig_msc))<12,0,12))))),NULL)) as TR_ECID_CLID, --refering to TR_Type_OMSC and CNL
CAST(IF(UPPER(SUBSTR(trim(orig_msc),1,1))='D',
    UPPER(SUBSTR(TRIM(orig_msc),12)), 
    if(UPPER(SUBSTR(trim(orig_msc),1,1))='C',
       UPPER(SUBSTR(TRIM(orig_msc),12,(LENGTH(orig_msc)-(if(LENGTH(trim(orig_msc))<12,0,12))) ) ), NULL)) AS VARCHAR(10)) AS TR_ECID_CLID,

-- below two fields will be used to JOIN with npa_nxx_gg_dim_state_mps table in next pass
if(UPPER(SUBSTR(trim(orig_msc),1,1))='1',SUBSTR(trim(orig_msc),2,3),NULL) as TR_NPA, --refering to TR_Type_OMSC
if(UPPER(SUBSTR(trim(orig_msc),1,1))='1',SUBSTR(trim(orig_msc),5,3),NULL) as TR_NXX, --refering to TR_Type_OMSC

SUBSTR(if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL),1,1) as PRX1, --refering to BNum
SUBSTR(if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL),1,2) as PRX2, --refering to BNum
SUBSTR(if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL),1,3) as PRX3, --refering to BNum
SUBSTR(if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL),1,4) as PRX4, --refering to BNum
SUBSTR(if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL),1,5) as PRX5, --refering to BNum
SUBSTR(if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL),1,6) as PRX6, --refering to BNum
SUBSTR(if(LENGTH(trim(cast(other_msisdn as string)))>11 AND other_msisdn_sc IS NULL,trim(cast(other_msisdn as string)),NULL),1,7) as PRX7, --refering to BNum
---- Data File fields
file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records
FROM ext_cdrdm.siumsmsraw_temp1 tmp
-- Uncomment below two JOINS when DIM_SUBSCRIBER & DIM_SERVICE_PROVIDER tables are available to calculate OCN
LEFT OUTER JOIN ext_cdrdm.subscriber_ocn sbocn
ON tmp.ocn_Other_MSISDN = sbocn.subscriber_no

LEFT OUTER JOIN cdrdm.dim_interaction_source iact
ON (tmp.NPA_var = trim(iact.NPA) AND tmp.NXX_var = trim(iact.NXX))
;

------------------------------------------------------------------------------
-- Mapping #3 - m_BIDW_CDR_Ins_SMS_HP_FACT
INSERT INTO TABLE cdrdm.FACT_SMS_CDR PARTITION (local_subscriber_date)
SELECT
a.subscriber_no,
a.event_record_id,
a.spid,
a.orig_msc,
a.other_msisdn,
a.orig_msc_zone_indicator,
a.rate,
a.charged_party,
a.action,
a.rule,
a.transaction_id,
a.charge_result,
a.balance,
a.rate_plan,
a.on_net,
a.originating_service_grade,
a.terminating_service_grade,
a.sequence_number,
a.local_subscriber_timestamp,
a.imsi,
a.ban,
a.language,
a.bill_cycle_date,
a.orignating_subscriber_loc,  -- a.originating_subscriber_location,
a.terminating_subscriber_loc, -- a.terminating_subscriber_location,
a.rating_rule_description_1,
a.rating_rule_description_2,
a.origination_eq_type,
a.destination_eq_type,
a.date_1,
a.ocn,
a.other_msisdn_sc,
a.TR_Type_OMSC,
a.TR_OMSC_PLMN,
a.TR_LAC_TAC,
a.TR_ECID_CLID,
b.country_code as TR_MSC_TYPE,
CAST(IF(d_7.country_name is not NULL,d_7.country_name,IF(d_6.country_name is not NULL,d_6.country_name,IF(d_5.country_name is not NULL,d_5.country_name,IF(d_4.country_name is not NULL,d_4.country_name,IF(d_3.country_name is not NULL,d_3.country_name,IF(d_2.country_name is not NULL,d_2.country_name,IF(d_1.country_name is not NULL,d_1.country_name,'NOT_FOUND'))))))) AS VARCHAR(30)) AS TR_DCODE_COUNTRY,
c.account_sub_type as TR_ACCOUNT_SUB_TYPE,
c.company_code as TR_COMPANY_CODE,
c.franchise_cd as TR_FRANCHISE_CD,
c.tr_ba_account_type as TR_BA_ACCOUNT_TYPE,
CAST(IF(sbscrbr_no_lessthan_1G=1 AND other_msisdn_equal_410002=1 AND rtng_rl_dscrptn_1_2_shorterthan_10chr=1 AND charged_party='O','EMOC',if(sbscrbr_no_lessthan_1G=1 AND other_msisdn_equal_410002=1 AND rtng_rl_dscrptn_1_2_shorterthan_10chr=0 AND charged_party='O','EMOS',if(sbscrbr_no_lessthan_1G=1 AND other_msisdn_equal_410002=1 AND charged_party='T','EMT',if(sbscrbr_no_lessthan_1G=1 AND other_msisdn_equal_410002=0 AND charged_party='O' AND (other_msisdn_sc IS NULL OR other_msisdn_sc = ''),'ESOS',if(sbscrbr_no_lessthan_1G=1 AND other_msisdn_equal_410002=0 AND charged_party='T' AND (other_msisdn_sc IS NULL OR other_msisdn_sc = ''),'ESTS',if(sbscrbr_no_lessthan_1G=1 AND other_msisdn_equal_410002=0 AND charged_party='O' AND (other_msisdn_sc is not NULL OR other_msisdn_sc <> ''),'ESOC',if(sbscrbr_no_lessthan_1G=1 AND other_msisdn_equal_410002=0 AND charged_party='T' AND (other_msisdn_sc is not NULL OR other_msisdn_sc <> ''),'ESTC',if(sbscrbr_no_lessthan_1G=0 AND other_msisdn_equal_410002=1 AND rtng_rl_dscrptn_1_2_shorterthan_10chr=1 AND charged_party='O','SMOC',if(sbscrbr_no_lessthan_1G=0 AND other_msisdn_equal_410002=1 AND rtng_rl_dscrptn_1_2_shorterthan_10chr=0 AND charged_party='O','SMOS',if(sbscrbr_no_lessthan_1G=0 AND other_msisdn_equal_410002=1 AND charged_party='T','SMT',if(sbscrbr_no_lessthan_1G=0 AND other_msisdn_equal_410002=0 AND charged_party='O' AND (other_msisdn_sc IS NULL OR other_msisdn_sc = ''),'SSOS',if(sbscrbr_no_lessthan_1G=0 AND other_msisdn_equal_410002=0 AND charged_party='T' AND (other_msisdn_sc IS NULL OR other_msisdn_sc = ''),'SSTS',if(sbscrbr_no_lessthan_1G=0 AND other_msisdn_equal_410002=0 AND charged_party='O' AND (other_msisdn_sc is not NULL OR other_msisdn_sc <> '') ,'SSOC',if(sbscrbr_no_lessthan_1G=0 AND other_msisdn_equal_410002=0 AND charged_party='T' AND (other_msisdn_sc is not NULL OR other_msisdn_sc <> '') ,'SSTC','OTHR')))))))))))))) AS VARCHAR(5)) AS MTYPE,

file_name,
record_start,
record_end,
record_type,
family_name,
version_id,
file_time,
file_id,
switch_name,
num_records,
from_unixtime(unix_timestamp()) AS insert_timestamp,
CONCAT(SUBSTR(local_subscriber_timestamp,1,4),"-",SUBSTR(local_subscriber_timestamp,6,2),"-",SUBSTR(local_subscriber_timestamp,9,2)) as local_subscriber_date
FROM ext_cdrdm.siumsmsraw_temp2 a 
LEFT OUTER JOIN cdrdm.npa_nxx_gg_dim_state_mps b 
ON (trim(a.TR_NPA) = trim(b.NPA) AND trim(a.TR_NXX) = trim(b.NXX)) 
LEFT OUTER JOIN 
   (SELECT account_sub_type, company_code, franchise_cd, tr_ba_account_type, MPS_CUST_SEQ_NO, product_type, ban, subscriber_no 
    FROM cdrdm.dim_MPS_CUST_1 WHERE MPS_CUST_SEQ_NO = 1 AND product_type = 'C') c 
ON (a.ban = c.ban AND cast(a.subscriber_no as VARCHAR(30)) = trim(c.subscriber_no))

LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_1 
ON (trim(a.PRX1) = trim(d_1.dial_code)) 
LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_2 
ON (trim(a.PRX2) = trim(d_2.dial_code)) 
LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_3 
ON (trim(a.PRX3) = trim(d_3.dial_code)) 
LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_4 
ON (trim(a.PRX4) = trim(d_4.dial_code)) 
LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_5 
ON (trim(a.PRX5) = trim(d_5.dial_code)) 
LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_6 
ON (trim(a.PRX6) = trim(d_6.dial_code)) 
LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_7 
ON (trim(a.PRX7) = trim(d_7.dial_code));


DROP TABLE IF EXISTS ext_cdrdm.subscriber_ocn;
DROP TABLE IF EXISTS ext_cdrdm.siumsmsraw_temp1;
DROP TABLE IF EXISTS ext_cdrdm.siumsmsraw_temp2;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--FROM ext_cdrdm.siumsmsraw_temp2 a LEFT OUTER JOIN cdrdm.npa_nxx_gg_dim_state_mps b ON (217 = b.NPA AND 448 = b.NXX) LEFT OUTER JOIN cdrdm.dim_MPS_CUST_1 c ON ("5319de1a0c4141a8c510e9dd847548" = c.subscriber_no) LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_1 ON ("87376" = d_1.dial_code) LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_2 ON (a.PRX2 = d_2.dial_code) LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_3 ON (a.PRX3 = d_3.dial_code) LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_4 ON (a.PRX4 = d_4.dial_code) LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_5 ON (a.PRX5 = d_5.dial_code) LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_6 ON (a.PRX6 = d_6.dial_code) LEFT OUTER JOIN ela_v21.ld_numbering_plan_gg d_7 ON (a.PRX7 = d_7.dial_code);


