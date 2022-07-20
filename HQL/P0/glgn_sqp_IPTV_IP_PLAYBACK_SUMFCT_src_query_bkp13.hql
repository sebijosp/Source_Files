select 
<SELECT_COLS_PLACEHOLDER> 
from iptv.ip_playback_sumfct 
where event_date >= cast(date_add(cast(current_date as String), -60) as String)
and ( ( ( Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) > cast('2022-04-02 06:12:02.401' as TIMESTAMP) and
Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) <= cast('2022-04-21 12:04:26.408' as TIMESTAMP)) ) )
and event_date = '2022-04-14'
