select 
<SELECT_COLS_PLACEHOLDER> 
from iptv.ip_playback_sumfct 
where event_date >= cast(date_add(cast(current_date as String), -60) as String)
and ( ( ( Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) > cast('2022-03-23 07:01:59.454' as TIMESTAMP) and
Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) <= cast('2022-03-24 23:59:99.999' as TIMESTAMP)) ) )
