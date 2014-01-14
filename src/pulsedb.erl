-module(pulsedb).

-include("pulsedb.hrl").


-export_types([db/0, utc/0, source_name/0, column_name/0, tick/0]).


-export([open/1, append/2, read/3, close/1]).
-export([info/1]).
-export([parse_query/1]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) ->
  pulsedb_disk:open(Path).


-spec append(tick() | [tick()], db()) -> db().
append({Name, UTC, Value, Tags} = Tick, #db{date = Date} = DB) when is_binary(Name), 
  is_integer(UTC), UTC >= 0, UTC =< 4294967295,
  is_integer(Value), Value >= 0, Value =< 4294967295,
  is_list(Tags) ->
  UTCDate = pulsedb_time:date_path(UTC),
  {ok, DB1} = if
    Date == undefined orelse UTCDate == Date -> pulsedb_disk:append(Tick, DB);
    true ->
      {ok, DB_} = close(DB),
      pulsedb_disk:append(Tick, DB_)
  end,
  {ok, DB1};

append(Ticks, #db{} = DB) ->
  lists:foldl(fun(Tick, {ok, DB_}) ->
    append(Tick, DB_)
  end, {ok, DB}, Ticks).




-spec close(pulsedb:db()) -> pulsedb:db().
close(#db{} = DB) ->
  pulsedb_disk:close(DB).






% -export([open/1, read/2, info/1, append/2, merge/2, close/1]).





-spec read(Name::source_name(), Query::[{atom(),any()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.
read(Name, Query, #db{} = DB) ->
  Query1 = parse_query(Query),
  RequiredDates = required_dates(Query1),
  {ok, DB0} = close(DB),
  case load_ticks(RequiredDates, Name, Query1, DB0) of
    {ok, Ticks, DB1} ->
      {ok, Ticks, DB1};
    {error, _} = Error ->
      Error
  end.




parse_query([{from,From}|Query]) ->  [{from,pulsedb_time:parse(From)}|parse_query(Query)];
parse_query([{to,To}|Query])     ->  [{to,pulsedb_time:parse(To)}|parse_query(Query)];
parse_query([{Key,Value}|Query]) ->  [{Key,Value}|parse_query(Query)];
parse_query([])                  ->  [].


required_dates(Query) ->
  {from,From} = lists:keyfind(from,1,Query),
  {to,To} = lists:keyfind(to,1,Query),
  [pulsedb_time:date_path(T) || T <- lists:seq(From,To,86400)].


load_ticks([], _Name, _Query, DB) ->
  {ok, [], DB};

load_ticks([Date|Dates], Name, Query, DB) ->
  case pulsedb_disk:read(Name, Query, DB#db{date = Date}) of
    {ok, Ticks1, DB1} ->
      {ok, DB2} = pulsedb_disk:close(DB1),
      case load_ticks(Dates, Name, Query, DB2) of
        {ok, Ticks2, DB3} ->
          {ok, Ticks1++Ticks2, DB3};
        {error, _} = Error ->
          Error
      end;        
    {error, _} = Error ->
      Error
  end.








% -spec merge([pulsedb:tick()], pulsedb:db()) -> {ok, pulsedb:db()} | {error, Reason::any()}.

% merge([], #db{} = DB) ->
%   {ok, 0, DB};

% merge([#tick{utc = FirstUTC, name = Name}|_] = Ticks, #db{path = Path} = DB) ->
%   {ok, RDB0} = open(Path),
%   From = pulsedb_time:parse(FirstUTC),
%   To = From+86400,
%   {ok, AvailTicks, RDB1} = read([{from,From},{to,To},{name,Name}], RDB0),
%   close(RDB1),
%   WriteTicks = [Tick || #tick{utc = UTC} = Tick <- Ticks, not lists:keymember(UTC, #tick.utc, AvailTicks)],
%   {ok, DB1} = pulsedb_disk:append(WriteTicks, DB),
%   {ok, length(WriteTicks), DB1}.





-spec info(pulsedb:db()|file:filename()) -> [{sources,[{pulsedb:source_name(),[{columns,[pulsedb:column_name()]}]}]}].
info(#db{} = DB) ->
  pulsedb_disk:info(DB);

info(Path) ->
  {ok, DB} = pulsedb:open(Path),
  Info = pulsedb_disk:info(DB),
  pulsedb:close(DB),
  Info.




