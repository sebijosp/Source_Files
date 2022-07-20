DROP TABLE HEM.WIRELINE_NODE_DIM;
CREATE TABLE HEM.WIRELINE_NODE_DIM
(
network_node_key        bigint,
cmts_name               string,
mac_domain              string,
md_ifindex              string,
card_port               string,
port_name               string,
port_type               string,
rtn_seg                 string,
fwd_seg                 string,
HDP_INSERT_TS           TIMESTAMP,
HDP_UPDATE_TS           TIMESTAMP

)
PARTITIONED BY (DATE_KEY DATE)
STORED AS ORC;
