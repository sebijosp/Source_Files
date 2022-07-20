select 
<SELECT_COLS_PLACEHOLDER> 
from iptv.ip_playback_sumfct 
where event_date >= cast(date_add(cast(current_date as String), -60) as String)
and ( ( ( Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) > cast('2022-05-20 05:49:39.176' as TIMESTAMP) and
Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) <= cast('2022-05-28 12:04:26.408' as TIMESTAMP)) ) )
and event_date between '2022-05-11' and '2022-05-20'
