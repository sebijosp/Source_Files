create or replace view data_pods_nonpi.vw_web_pod_lookup1 as
select
webpage,
dynamic_content,
join_key,
webpage_category1,
webpage_category2,
webpage_category3
from data_pods.web_pod_lookup1;
