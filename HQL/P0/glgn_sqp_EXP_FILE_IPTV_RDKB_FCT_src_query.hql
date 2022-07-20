 select

<SELECT_COLS_PLACEHOLDER>

from iptv.rdkb_fct t
where  t.Event_Date >= date_add(current_date,-7)

