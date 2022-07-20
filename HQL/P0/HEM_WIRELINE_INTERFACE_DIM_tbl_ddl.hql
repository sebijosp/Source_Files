!echo Attempt to drop HEM.WIRELINE_INTERFACE_DIM;

DROP TABLE IF EXISTS HEM.WIRELINE_INTERFACE_DIM;

!echo creating HEM.WIRELINE_INTERFACE_DIM;

CREATE TABLE HEM.WIRELINE_INTERFACE_DIM(
 INTERFACE_KEY            bigint,
 cmts                    string,
 INTERFACE_TYPE                  int,
 INTERFACE_NAME                string,
 INTERFACE_INDEX                 int,
 SOURCE_TYPE                 string,
 INTERFACE_SPEED                 bigint,
 channel_width           bigint,
 channel_frequency       bigint,
 channel_modulation      int,
 CRNT_F                  STRING,
 EFF_START_DT            DATE,
 EFF_END_DT              DATE,
 HDP_INSERT_TS           TIMESTAMP,
 HDP_UPDATE_TS           TIMESTAMP
)
STORED AS ORC;

!echo re-creating HEM.WIRELINE_INTERFACE_STG;
DROP TABLE IF EXISTS HEM.WIRELINE_INTERFACE_STG;
CREATE TABLE HEM.WIRELINE_INTERFACE_STG LIKE HEM.WIRELINE_INTERFACE_DIM;

!echo re-creating HEM.WIRELINE_INTERFACE_STG_MERGE;
DROP TABLE IF EXISTS HEM.WIRELINE_INTERFACE_STG_MERGE;
CREATE TABLE HEM.WIRELINE_INTERFACE_STG_MERGE LIKE HEM.WIRELINE_INTERFACE_DIM;
