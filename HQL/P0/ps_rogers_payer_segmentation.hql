CREATE TABLE IF NOT EXISTS MT_PRICING.ROGERS_PAYER_SEGMENTATION       
(       
LST_ECID	BIGINT,
BAN	BIGINT,
SUBSCRIBER_NO	VARCHAR(20),
SEG_SUB_GROUP	VARCHAR(20),
SUB_MARKET_CD	VARCHAR(20),
PLAN_ERA	VARCHAR(150),
PP_SHARING_TYPE_DESC	VARCHAR(150),
BL_SUB_CNT	INT,
PRICE_PLAN	VARCHAR(30),
PLAN_DESCRIPTION	VARCHAR(150),
LST_PP_EFFECTIVE_DATE	DATE,
PRICE_PLAN_TYPE_1	VARCHAR(255),
PRICE_PLAN_TYPE_2	VARCHAR(255),
SUBS_CATEGORY	VARCHAR(50),
PPLAN_SERIES_TYPE	INT,
PPLAN_SERIES_DESC	VARCHAR(200),
PPLAN_SERIES_TYPE_DESC	VARCHAR(150),
SE_IND	INT,
LST_INIT_ACTIVATION_DATE	DATE,
COMMIT_START	DATE,
COMMIT_END	DATE,
LST_COMMIT_ORIG_NO_MONTH	INT,
TERM_WINDOW	VARCHAR(255),
MSD	VARCHAR(40),
COMPANY_CODE	VARCHAR(40),
MSF	DOUBLE,
PLAN_RC	DOUBLE,
NON_PLAN_RC	DOUBLE,
DISCOUNT_RC	DOUBLE,
MDT_RC	DOUBLE,
BAN_DATA_TOTAL_PRE	DOUBLE,
BAN_DATA_USAGE_ROAM_PRE	DOUBLE,
BAN_DATA_USAGE_PRE	DOUBLE,
BAN_DATA_USAGE_OTHER_PRE	DOUBLE,
BAN_DATA_USG_OTH_KB_PRE	DOUBLE,
DATA_TOTAL_PRE	DOUBLE,
DATA_USAGE_ROAM_PRE	DOUBLE,
DATA_USAGE_PRE	DOUBLE,
DATA_USAGE_OTHER_PRE	DOUBLE,
DATA_USG_OTH_KB_PRE	DOUBLE,
BAN_DATA_BUCKET1	DOUBLE,
HAS_DATA_SOC_IN_BAN_POST	DOUBLE,
BAN_DATA_TOPUPS	DOUBLE,
BAN_MDTS	DOUBLE,
BAN_DATABONUSES	DOUBLE,
SUB_DATA_BUCKET1	DOUBLE,
HAS_DATA_SOC_POST	DOUBLE,
SUB_DATA_TOPUPS	DOUBLE,
SUB_MDTS	DOUBLE,
SUB_DATABONUSES	DOUBLE,
BAN_MM_ARPA_PRE_ORIG	DOUBLE,
BAN_BASE_PRE	DOUBLE,
BAN_BASE_DATA_PRE	DOUBLE,
BAN_MSF	DOUBLE,
BAN_DATA_BROWSING_PRE	DOUBLE,
BAN_DATA_OTHER_PRE	DOUBLE,
BAN_DATA_OVERAGE_PRE	DOUBLE,
BAN_DATA_ROAM_PRE	DOUBLE,
BAN_FEES_PRE	DOUBLE,
BAN_LD_CAN_PRE	DOUBLE,
BAN_LD_US_INTL_PRE	DOUBLE,
BAN_SMS_PRE	DOUBLE,
BAN_SMS_US_INTL_PRE	DOUBLE,
BAN_VOICE_OVERAGE_PRE	DOUBLE,
BAN_VOICE_ROAM_PRE	DOUBLE,
BAN_WES_PRE	DOUBLE,
BAN_HPG_PRE	DOUBLE,
MM_ARPU_PRE_ORIG	DOUBLE,
BASE_PRE	DOUBLE,
BASE_DATA_PRE	DOUBLE,
DATA_BROWSING_PRE	DOUBLE,
DATA_OTHER_PRE	DOUBLE,
DATA_OVERAGE_PRE	DOUBLE,
DATA_ROAM_PRE	DOUBLE,
FEES_PRE	DOUBLE,
LD_CAN_PRE	DOUBLE,
LD_US_INTL_PRE	DOUBLE,
SMS_PRE	DOUBLE,
SMS_US_INTL_PRE	DOUBLE,
VOICE_OVERAGE_PRE	DOUBLE,
VOICE_ROAM_PRE	DOUBLE,
WES_PRE	DOUBLE,
HPG_PRE	DOUBLE,
LST_PAYEE_IND	VARCHAR(20),
SUBSIDY_AMT	DOUBLE,
SUBSIDY_ACTIVITY	VARCHAR(20),
SUBSIDY_START_DATE	DATE,
SUBSIDY_END_DATE	DATE,
PAYEE	VARCHAR(200),
PAYEE_TYPE	VARCHAR(200),
BAN_PLAN_RC	DOUBLE,
BAN_NON_PLAN_RC	DOUBLE,
BAN_DISCOUNT_RC	DOUBLE,
BAN_MDT_RC	DOUBLE,
BAN_MRC	DOUBLE,
BAN_PLAN_RC_NET	DOUBLE,
MRC	DOUBLE,
PLAN_RC_NET	DOUBLE,
ARPA_INDICATOR	VARCHAR(100),
ARPU_INDICATOR	VARCHAR(100),
BAN_MM_ARPA_PRE	DOUBLE,
MM_ARPU_PRE	DOUBLE,
BAN_DATA_BUCKET	DOUBLE,
SUB_DATA_BUCKET	DOUBLE,
BAN_DATA_USAGE	DOUBLE,
DATA_USAGE	DOUBLE,
BL_DATA_PCT_OLD	DOUBLE,
ARPU_VALUE_BAND	VARCHAR(50),
ARPA_VALUE_BAND	VARCHAR(50),
COMPANY	VARCHAR(200),
MSD_FLAG	INT,
TVM_IND	INT,
RET_IND	INT,
PAYEE_TYPE_00	VARCHAR(200),
PAYEE_TYPE_0	VARCHAR(200),
PAYEE_TYPE_1	VARCHAR(200),
PAYEE_TYPE_2	VARCHAR(200),
PAYEE_TYPE_3	VARCHAR(200),
PAYEE_TYPE_4	VARCHAR(200),
PAYEE_TYPE_5	VARCHAR(200),
PAYEE_TYPE_6	VARCHAR(200),
PAYEE_TYPE_FINAL	VARCHAR(200),
CTN_HIGH_RISK_FLAG	INT,
CTN_DECILE_FLAG	VARCHAR(20),
HIGH_RISK_FLAG	INT,
REGION	VARCHAR(20),
BAN_REGION	VARCHAR(20),
SUBS_CATEGORY_NEW	VARCHAR(70),
PP_CAT_GRP_RNK	INT,
PP_CAT_CODE	VARCHAR(50),
PP_CAT_CODE_OLD	VARCHAR(70),
PP_CAT_GRP_RNK_OLD	INT,
SUBS_CATEGORY_NEW2	VARCHAR(70),
DESJ_PCT	DOUBLE,
AALS_DISC	DOUBLE,
TERM_STATUS	VARCHAR(70),
ROCKET_HUB_FLAG	INT,
EMPLOYEE_FLAG	INT,
IDV_DISCOUNT_FLAG	INT,
CAN_US_ADD_FLAG	VARCHAR(70),
COL_DELINQ_STATUS	VARCHAR(20),
AR_BALANCE	DOUBLE,
BAN_STATUS	VARCHAR(20),
CREDIT_CLASS	VARCHAR(20),
CREDIT_CLASS_DESC	VARCHAR(150),
CREDIT_CLASS_GROUP	VARCHAR(50),
AR_BALANCE_TYPE	VARCHAR(70),
LINES	INT,
B_DATA_USAGE	DOUBLE,
B_DATA_BUCKET	DOUBLE,
B_ARPA_PRE	DOUBLE,
SE_LINES	DOUBLE,
SE_B_DATA_USAGE	DOUBLE,
SE_B_DATA_BUCKET	DOUBLE,
SE_B_ARPA_PRE	DOUBLE,
INF_SEC_LINES	DOUBLE,
SUB_DATA_BUCKET_NEW	DOUBLE,
SUB_DATA_BUCKET_NEW_WBONUS	DOUBLE,
DATA_PCT	DOUBLE,
RATE_CARD	VARCHAR(400),
BUCKET_MB	DOUBLE,
BUCKET_MIN	DOUBLE,
BUCKET_MAX	DOUBLE,
MM_IND	DOUBLE,
SEG_BUCKET_INDEX	DOUBLE,
MSF_R	DOUBLE,
NA_AAL_DISCOUNT	DOUBLE,
QC_PL_DISCOUNT	DOUBLE,
QC_AAL_DISCOUNT	DOUBLE,
MW_PL_DISCOUNT	DOUBLE,
MW_AAL_DISCOUNT	DOUBLE,
INMARKET_SPEND	DOUBLE,
ARPU_DELTA	DOUBLE,
ARPA_DELTA	DOUBLE,
ARPU_DELTA_BANDS	VARCHAR(70),
ARPA_DELTA_BANDS	VARCHAR(70),
CTN_SEGMENT_BAU	VARCHAR(70),
SEGMENT_BAU	VARCHAR(70),
COST_PER_GB_CURRENT	DOUBLE,
COST_PER_GB_RATECARD	DOUBLE,
CPG_DIFFERENTIAL	DOUBLE,
BAN_CPG_DIFFERENTIAL	DOUBLE,
SUB_CPG_SEGMENT	VARCHAR(70),
MONTHS_LEFT	DOUBLE,
ECF_REMAINING	DOUBLE,
RATE_CARD_SEG	VARCHAR(400),
BUCKET_MB_SEG	DOUBLE,
BUCKET_MIN_SEG	DOUBLE,
BUCKET_MAX_SEG	DOUBLE,
MM_IND_SEG	DOUBLE,
MSF_R_SEG	DOUBLE,
SEG_BUCKET_INDEX_SEG	DOUBLE,
INMARKET_SPEND_SEG	DOUBLE,
ARPU_DELTA_SEG	DOUBLE,
ARPA_DELTA_SEG	DOUBLE,
CTN_SEGMENT_SEG	VARCHAR(70),
SEGMENT_SEG	VARCHAR(70),
BKTUSG_VS_TARGET	VARCHAR(50),
LOAD_TS	TIMESTAMP
)  
PARTITIONED BY (EXTRACTION_DATE  DATE)
STORED AS ORC;
