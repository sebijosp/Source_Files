select
<SELECT_COLS_PLACEHOLDER>
from iptv.vod_rental_sumfct
where event_date >= cast(date_add(cast(current_date as String), -30) as String)
and ( <SQP_DELTA_WHERE_CONDITION_PLACEHOLDER> )
