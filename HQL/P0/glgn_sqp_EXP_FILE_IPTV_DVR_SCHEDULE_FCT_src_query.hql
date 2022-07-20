select

<SELECT_COLS_PLACEHOLDER>

from iptv.dvr_schedule_fct fct LATERAL VIEW explode(recordings) recordingsArray as recording
     where fct.event_date >= date_add(current_date,-7)
