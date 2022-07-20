set hive.execution.engine=mr;
set hive.support.quoted.identifiers=none;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set mapreduce.map.memory.mb=9000;
set mapreduce.map.java.opts=-Xmx8500m;


use hash_lookup;

-- Drop provious temp tables in case the processed failed
DROP TABLE IF EXISTS ecid_ban_can_prev;
DROP TABLE IF EXISTS ecid_ban_can_new;

-- Create Dest table (should happen only for the first run)
CREATE TABLE IF NOT EXISTS ecid_ban_can (
    ECID INT,
    ACCOUNT VARCHAR(30),
    HASH_ACCOUNT VARCHAR(64),
    SYSTEM_SRC_CD VARCHAR(10)
)
STORED AS ORC;
-- create hist table 
CREATE TABLE IF NOT EXISTS HIST_ecid_ban_can (
 ECID INT,
    ACCOUNT VARCHAR(30),
    HASH_ACCOUNT VARCHAR(64),
    SYSTEM_SRC_CD VARCHAR(10)
) PARTITIONED BY (BUILD_DATE DATE)
STORED AS ORC;
-- Create temp table for new extract
CREATE TABLE IF NOT EXISTS ecid_ban_can_new (
    ECID INT,
    ACCOUNT VARCHAR(30),
    HASH_ACCOUNT VARCHAR(64),
    SYSTEM_SRC_CD VARCHAR(10)
)
STORED AS ORC;
-- Populate temp table with new data
INSERT OVERWRITE TABLE ecid_ban_can_new
SELECT DISTINCT
    a.enterprise_id AS ecid,
    b.SRC_ACCT_KEY AS account,
    CASE WHEN b.SRC_ACCT_KEY IS NULL THEN NULL ELSE reflect('org.apache.commons.codec.digest.DigestUtils', 'sha256Hex',CAST(b.SRC_ACCT_KEY AS STRING)) END AS hash_account,
    b.system_src_cd
FROM
    ela_rcis.ec_cust_acct AS A
    INNER JOIN ela_rcis.ec_account AS B On A.ACCT_ID = B.ACCT_ID
WHERE
    A.EXPIRY_DT IS NULL OR A.EXPIRY_DT = 'NULL';

-- insert into hist table
INSERT OVERWRITE TABLE HIST_ECID_BAN_CAN PARTITION (BUILD_DATE)
SELECT ECID, ACCOUNT, HASH_ACCOUNT, SYSTEM_SRC_CD,to_date(from_unixtime(unix_timestamp())) FROM ecid_ban_can_new;

-- Swap the old and new tables
ALTER TABLE ecid_ban_can RENAME TO ecid_ban_can_prev;
ALTER TABLE ecid_ban_can_new RENAME TO ecid_ban_can;

-- Drop the old and temp tables
DROP TABLE IF EXISTS ecid_ban_can_prev;
DROP TABLE IF EXISTS ecid_ban_can_new;
