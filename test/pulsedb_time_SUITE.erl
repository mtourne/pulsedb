-module(pulsedb_time_SUITE).
-compile(export_all).


all() ->
  [{group,time}].


groups() ->
  [{time,[parallel], [
    parse_string_date,
    parse_string_datetime
  ]}].


parse_string_date(_) -> 0 = pulsedb_time:parse("1970-01-01").
parse_string_datetime(_) -> 46444 = pulsedb_time:parse("1970-01-01 12:54:04").
