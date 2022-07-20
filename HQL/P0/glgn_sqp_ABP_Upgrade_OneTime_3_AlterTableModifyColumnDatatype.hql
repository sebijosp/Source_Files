!echo "executing alters for PPV_BILLING_FACT";
ALTER TABLE iptv.hist_PPV_BILLING_FACT         CHANGE DEVICE_ID DEVICE_ID VARCHAR(255);
ALTER TABLE iptv.PPV_BILLING_FACT              CHANGE DEVICE_ID DEVICE_ID VARCHAR(255);
ALTER TABLE ext_iptv.PPV_BILLING_FACT_sqp_imp  CHANGE DEVICE_ID DEVICE_ID VARCHAR(255);
ALTER TABLE iptv.hist_PPV_BILLING_FACT         CHANGE CUSTOMER_ID                 CUSTOMER_ID           bigint;
ALTER TABLE iptv.hist_PPV_BILLING_FACT         CHANGE SRC_SUBSCRIBER_ID           SRC_SUBSCRIBER_ID     bigint;
ALTER TABLE iptv.PPV_BILLING_FACT              CHANGE CUSTOMER_ID                 CUSTOMER_ID           bigint;
ALTER TABLE iptv.PPV_BILLING_FACT              CHANGE SRC_SUBSCRIBER_ID           SRC_SUBSCRIBER_ID     bigint;
ALTER TABLE ext_iptv.PPV_BILLING_FACT_sqp_imp  CHANGE CUSTOMER_ID                 CUSTOMER_ID           bigint;
ALTER TABLE ext_iptv.PPV_BILLING_FACT_sqp_imp  CHANGE SRC_SUBSCRIBER_ID           SRC_SUBSCRIBER_ID     bigint;


!echo "executing alters for VOD_BILLING_FACT";
ALTER TABLE iptv.hist_vod_billing_fact         CHANGE DEVICE_ID DEVICE_ID VARCHAR(255);
ALTER TABLE iptv.vod_billing_fact              CHANGE DEVICE_ID DEVICE_ID VARCHAR(255);
ALTER TABLE ext_iptv.vod_billing_fact_sqp_imp  CHANGE DEVICE_ID DEVICE_ID VARCHAR(255);
ALTER TABLE iptv.hist_vod_billing_fact         CHANGE CUSTOMER_ID                 CUSTOMER_ID           bigint;
ALTER TABLE iptv.hist_vod_billing_fact         CHANGE SRC_SUBSCRIBER_ID           SRC_SUBSCRIBER_ID bigint;
ALTER TABLE iptv.vod_billing_fact              CHANGE CUSTOMER_ID                 CUSTOMER_ID           bigint;
ALTER TABLE iptv.vod_billing_fact              CHANGE SRC_SUBSCRIBER_ID           SRC_SUBSCRIBER_ID bigint;
ALTER TABLE ext_iptv.vod_billing_fact_sqp_imp  CHANGE CUSTOMER_ID                 CUSTOMER_ID           bigint;
ALTER TABLE ext_iptv.vod_billing_fact_sqp_imp  CHANGE SRC_SUBSCRIBER_ID           SRC_SUBSCRIBER_ID bigint;
