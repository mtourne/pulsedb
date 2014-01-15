-module(pulsedb).

-include("pulsedb.hrl").


-export_types([db/0, utc/0, source_name/0, tick/0]).


-export([open/1, open/2, append/2, read/3, close/1]).
-export([info/1]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) ->
  open(Path, []).


-spec open(Path::file:filename()|atom(), Options::list()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path, Options) when is_list(Path) ->
  open(undefined, [{url, "file://"++Path}|Options]);

open(Name, Options) when is_atom(Name) ->
  case Name of
    undefined -> pulsedb_disk:open(proplists:get_value(url, Options));
    _ -> pulsedb_worker:start_link(Name, Options)
  end.


-spec append(tick() | [tick()], db()) -> db().
append(Ticks, DB) ->
  validate(Ticks),
  if
    is_pid(DB) -> 
      pulsedb_worker:append(Ticks, DB);
    is_tuple(DB) -> 
      Module = element(2,DB),
      Module:append(Ticks, DB)
  end.


validate([Tick|Ticks]) ->
  validate(Tick),
  validate(Ticks);
validate([]) ->
  ok;
validate({Name,UTC,Value,Tags} = Tick) ->
  is_binary(Name) orelse is_atom(Name) orelse error({wrong_name,Tick}),
  is_integer(UTC) andalso UTC >= 0 andalso UTC =< 4294967295 orelse error({wrong_utc,Tick}),
  is_integer(Value) andalso Value >= 0 andalso Value =< 4294967295 orelse error({wrong_name,Tick}),
  is_list(Tags) orelse error({wrong_tags,Tick}),
  ok.




-spec close(pulsedb:db()) -> pulsedb:db().
close(DB) ->
  if
    is_pid(DB) -> pulsedb_worker:stop(DB);
    is_tuple(DB) -> pulsedb_disk:close(DB)
  end.






% -export([open/1, read/2, info/1, append/2, merge/2, close/1]).





-spec read(Name::source_name(), Query::[{atom(),any()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.
read(Name, Query, DB) ->
  Query1 = parse_query(Query),
  if
    is_pid(DB) -> pulsedb_worker:read(Name, Query1, DB);
    is_tuple(DB) -> pulsedb_disk:read(Name, Query1, DB)
  end.





parse_query([{from,From}|Query]) ->  [{from,pulsedb_time:parse(From)}|parse_query(Query)];
parse_query([{to,To}|Query])     ->  [{to,pulsedb_time:parse(To)}|parse_query(Query)];
parse_query([{Key,Value}|Query]) ->  [{Key,Value}|parse_query(Query)];
parse_query([])                  ->  [].





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





-spec info(pulsedb:db()|file:filename()) -> term().
info(DB) when is_tuple(DB) ->
  pulsedb_disk:info(DB);

info(Path) ->
  {ok, DB} = pulsedb:open(Path),
  Info = pulsedb_disk:info(DB),
  pulsedb:close(DB),
  Info.




