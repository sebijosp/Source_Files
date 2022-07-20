drop table data_pods.dim_conv_web_pagecategory_dly purge;
CREATE TABLE data_pods.dim_conv_web_pagecategory_dly(
webpage                 varchar(255),
dynamic_content         varchar(255),
join_key                varchar(510),
webpage_category1       string,
webpage_category2       string,
webpage_category3       string)
tblproperties ("orc.compress"="SNAPPY");
