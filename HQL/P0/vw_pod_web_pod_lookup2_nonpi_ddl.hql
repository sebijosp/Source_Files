create or replace view data_pods_nonpi.vw_web_pod_lookup2 as
select
webpage,
dynamic_content,
url,
join_key
from data_pods.web_pod_lookup2;
