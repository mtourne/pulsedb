-module(pulsedb).

-include("pulsedb.hrl").
-include("../include/pulsedb.hrl").


-export_types([db/0, utc/0, source_name/0, column_name/0, tick/0]).


-export([open/1, read/2, append/2, close/1]).

-export([parse_query/1]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) ->
  pulsedb_disk:open(Path).


-spec read(Query::[{atom(),any()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.
read(Query, #db{} = DB) ->
  Query1 = parse_query(Query),
  RequiredDates = required_dates(Query1),
  {ok, DB0} = close(DB),
  case load_ticks(RequiredDates, Query1, DB0) of
    {ok, Ticks, DB1} ->
      Ticks1 = filter_ticks(Query1, Ticks),
      {ok, Ticks1, DB1};
    {error, _} = Error ->
      Error
  end.



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
  case pulsedb_disk:read(Query, DB#db{date = Date}) of
    {ok, Ticks1, DB1} ->
      {ok, DB2} = pulsedb_disk:close(DB1),
      case load_ticks(Dates, Query, DB2) of
        {ok, Ticks2, DB3} ->
          {ok, Ticks1++Ticks2, DB3};
        {error, _} = Error ->
          Error
      end;        
    {error, _} = Error ->
      Error
  end.






-spec append([pulsedb:tick()], pulsedb:db()) -> {ok, pulsedb:db()} | {error, Reason::any()}.

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


-spec close(pulsedb:db()) -> {pulsedb:db()}.
close(#db{} = DB) ->
  pulsedb_disk:close(DB).







