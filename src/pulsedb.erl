-module(pulsedb).

-include("pulsedb.hrl").


-export_types([db/0, utc/0, source_name/0, column_name/0, tick/0]).


-export([open/1, read/2, append/2, close/1]).

-export([parse_query/1]).


open(Path) ->
  pulsedb_disk:open(Path).


read(Query, #db{} = DB) ->
  Query1 = parse_query(Query),
  RequiredDates = required_dates(Query1),
  {ok, DB0} = close(DB),
  {ok, Ticks, DB1} = load_ticks(RequiredDates, Query1, DB0),
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


required_dates(Query) ->
  {from,From} = lists:keyfind(from,1,Query),
  {to,To} = lists:keyfind(to,1,Query),
  [pulsedb_time:date_path(T) || T <- lists:seq(From,To,86400)].


load_ticks([], _Query, DB) ->
  {ok, [], DB};

load_ticks([Date|Dates], Query, DB) ->
  {ok, Ticks1, DB1} = pulsedb_disk:read(Query, DB#db{date = Date}),
  {ok, DB2} = pulsedb_disk:close(DB1),
  {ok, Ticks2, DB3} = load_ticks(Dates, Query, DB2),
  {ok, Ticks1++Ticks2, DB3}.







append([], #db{} = DB) ->
  {ok, DB};

append([#tick{utc = UTC}|_] = Ticks, #db{date = Date} = DB) ->
  case pulsedb_time:date_path(UTC) of
    Date ->
      pulsedb_disk:append(Ticks, DB);
    _ ->
      {ok, DB1} = pulsedb_disk:close(DB),
      pulsedb_disk:append(Ticks, DB1)
  end.


close(#db{} = DB) ->
  pulsedb_disk:close(DB).







