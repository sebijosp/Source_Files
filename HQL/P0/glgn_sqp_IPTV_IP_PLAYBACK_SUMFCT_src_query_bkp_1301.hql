select 
<SELECT_COLS_PLACEHOLDER> 
from iptv.ip_playback_sumfct 
where event_date >= cast(date_add(cast(current_date as String), -60) as String)
and ( ( ( Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) > cast('2021-12-14 12:37:38.4' as TIMESTAMP) and
Cast(IP_PLAYBACK_SUMFCT.HDP_UPDATE_TS as Timestamp) <= cast('2021-12-21 23:59:99.999' as TIMESTAMP)) ) )
and event_date = '2021-12-22'
