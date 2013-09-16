-module(pulsedb).

-include("pulsedb.hrl").


-export_types([db/0, utc/0, source_name/0, column_name/0, tick/0]).



-export([read/2]).

-export([parse_query/1]).


read(Query, #db{} = DB) ->
  Query1 = parse_query(Query),
  {ok, Ticks, DB1} = pulsedb_disk:read(Query1, DB),
  Ticks1 = filter_ticks(Query1, Ticks),
  {ok, Ticks1, DB1}.



filter_ticks([], Ticks) -> Ticks;
filter_ticks(_, []) -> [];
filter_ticks([{from,From}|Query], Ticks) ->
  Ticks1 = lists:dropwhile(fun(#tick{utc = UTC}) -> UTC < From end, Ticks),
  filter_ticks(Query, Ticks1);
filter_ticks([{to,To}|Query], Ticks) ->
  Ticks1 = lists:takewhile(fun(#tick{utc = UTC}) -> UTC =< To end, Ticks),
  filter_ticks(Query, Ticks1);
filter_ticks([_|Query], Ticks) ->
  filter_ticks(Query, Ticks).


parse_query([{from,From}|Query]) ->  [{from,pulsedb_time:parse(From)}|parse_query(Query)];
parse_query([{to,To}|Query])     ->  [{to,pulsedb_time:parse(To)}|parse_query(Query)];
parse_query([{Key,Value}|Query]) ->  [{Key,Value}|parse_query(Query)];
parse_query([])                  ->  [].
