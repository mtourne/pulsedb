-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").


-export([open/1, append/2, close/1]).
% -export([write_events/3]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) ->
  try open0(Path)
  catch
    throw:Reply -> Reply
  end.

open0(Path) ->
  case filelib:is_regular(filename:join(Path,config_v1)) of
    true ->
      open_existing_db(Path);
    false ->
      create_new_db(Path)
  end.



% write_events(Path, Events, Options) ->
%   {ok, S0} = pulsedb_appender:open(Path, Options),
%   S1 = lists:foldl(fun(Event, State) ->
%         {ok, NextState} = pulsedb_appender:append(Event, State),
%         NextState
%     end, S0, Events),
%   ok = pulsedb_appender:close(S1).




create_new_db(Path) ->
  ConfigPath = filename:join(Path, config_v1),
  IndexPath = filename:join(Path, index_v1),
  DataPath = filename:join(Path, data_v1),

  case filelib:ensure_dir(ConfigPath) of
    ok -> ok;
    {error, Reason1} -> throw({error, {create_path_failed,ConfigPath,Reason1}})
  end,

  {ok, ConfigFd} = case file:open(ConfigPath, [binary,append,exclusive,raw]) of
    {ok, CFile_} -> {ok, CFile_};
    {error, Reason2} -> throw({error,{create_config_failed,ConfigPath,Reason2}})
  end,

  {ok, IndexFd} = file:open(IndexPath, [binary,append,exclusive,raw]),
  {ok, DataFd} = file:open(DataPath, [binary,append,exclusive,raw]),

  {ok, #db{path = Path, config_fd = ConfigFd, index_fd = IndexFd, data_fd = DataFd}}.


open_existing_db(Path) ->
  ConfigPath = filename:join(Path, config_v1),
  IndexPath = filename:join(Path, index_v1),
  DataPath = filename:join(Path, data_v1),

  {ok, ConfigBin} = file:read_file(ConfigPath),
  Sources = pulsedb_format:decode_config(ConfigBin),

  {ok, ConfigFd} = file:open(ConfigPath, [binary,append,raw]),

  {ok, IndexFd} = file:open(IndexPath, [binary,append,raw]),
  {ok, DataFd} = file:open(DataPath, [binary,append,raw]),
  {ok, #db{path = Path, config_fd = ConfigFd, index_fd = IndexFd, data_fd = DataFd, sources = Sources}}.



-spec append([pulsedb:tick()], pulsedb:db()) -> {ok, pulsedb:db()}.
append([], #db{} = DB) ->
  {ok, DB};

append([#tick{value = []}|_], #db{} = DB) ->
  {ok, DB};

append([#tick{name = Name, value = Value}|_] = Ticks, #db{} = DB) ->
  validate_ticks(Ticks),
  {ok, DB1} = append_config_if_required(Name, [Column || {Column,_} <- Value], DB),
  {ok, IndexInfo, DB2} = append_data(Ticks, DB1),
  {ok, DB3} = append_index(IndexInfo, DB2),
  {ok, DB3}.

append_config_if_required(Name, Columns, #db{sources = Sources, config_fd = ConfigFd} = DB) ->
  case lists:keyfind(Name, #source.name, Sources) of
    #source{} -> 
      {ok, DB};
    false ->
      Source = #source{source_id = length(Sources), name = Name, columns = Columns},
      Bin = pulsedb_format:encode_config(Source),
      file:write(ConfigFd, Bin),
      file:sync(ConfigFd),
      {ok, DB#db{sources = Sources ++ [Source]}}
  end.

append_data([#tick{name = Name}|_] = Ticks, #db{data_fd = DataFd, sources = Sources} = DB1) ->
  UTC1 = (hd(Ticks))#tick.utc,
  UTC2 = (lists:last(Ticks))#tick.utc,
  {ok, Offset} = file:position(DataFd, cur),
  Bin = pulsedb_format:encode_data(lists:keyfind(Name,#source.name,Sources), Ticks),
  file:write(DataFd, Bin),
  {ok, {UTC1,UTC2,Offset,iolist_size(Bin)}, DB1}.

append_index(_IndexInfo, DB1) ->
  {ok, DB1}.





validate_ticks([#tick{name = Name, utc = UTC}|Ticks]) ->
  validate_ticks(Ticks, Name, UTC).

validate_ticks([],_,_) -> ok;
validate_ticks([#tick{name = Name, utc = UTC1}|Ticks], Name, UTC) when UTC1 > UTC -> validate_ticks(Ticks, Name, UTC1);
validate_ticks([Tick|_], Name, UTC) -> error({wrong_tick,Name,UTC,Tick}).



-spec close(pulsedb:db()) -> ok.
close(#db{config_fd = ConfigFd, index_fd = IndexFd, data_fd = DataFd}) ->
  file:close(ConfigFd),
  file:close(IndexFd),
  file:close(DataFd),
  ok.








