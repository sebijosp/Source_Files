drop table data_pods.dim_conv_web_pageurl_dly purge;
CREATE TABLE data_pods.dim_conv_web_pageurl_dly(
webpage                 varchar(255),
dynamic_content         varchar(255),
url                     varchar(255),
join_key                varchar(510))
tblproperties ("orc.compress"="SNAPPY");
