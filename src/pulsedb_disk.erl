-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").


-export([open/1, append/2, read/3, close/1]).
-export([info/1]).
% -export([write_events/3]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) ->
  {ok, #db{path = Path}}.


open0(#db{path = Path, config_fd = undefined, date = Date} = DB, Mode) when Date =/= undefined ->
  case filelib:is_regular(filename:join([Path,Date,config_v3])) of
    true ->
      open_existing_db(DB#db{mode = Mode});
    false when Mode == read ->
      DB#db{mode = read};
    false when Mode == append ->
      create_new_db(DB#db{mode = append})
  end.


create_new_db(#db{path = Path, date = Date, mode = append} = DB) when Date =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v3]),
  DataPath = filename:join([Path, Date, data_v3]),

  case filelib:ensure_dir(ConfigPath) of
    ok -> ok;
    {error, Reason1} -> throw({error, {create_path_failed,ConfigPath,Reason1}})
  end,

  Opts = [binary,write,exclusive,raw],

  {ok, ConfigFd} = case file:open(ConfigPath, Opts) of
    {ok, CFile_} -> {ok, CFile_};
    {error, Reason2} -> throw({error,{open_config_failed,ConfigPath,Reason2}})
  end,

  {ok, DataFd} = file:open(DataPath, Opts),

  DB#db{path = Path, config_fd = ConfigFd, data_fd = DataFd, sources = []}.


read_file(Path) ->
  case file:read_file(Path) of
    {ok, Bin} -> Bin;
    {error, _} -> <<>>
  end.

open_existing_db(#db{path = Path, date = Date, mode = Mode} = DB) when Date =/= undefined, Mode =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v3]),
  DataPath = filename:join([Path, Date, data_v3]),

  DB1 = case DB#db.sources of
    undefined ->
      DB#db{sources = decode_config(read_file(ConfigPath))};
    _ ->
      DB
  end,

  Opts = case Mode of
    append -> [binary,read,write,raw];
    read -> [binary,read,raw]
  end,

  {ok, ConfigFd} = file:open(ConfigPath, Opts),
  {ok, DataFd} = file:open(DataPath, Opts),

  DB1#db{config_fd = ConfigFd, data_fd = DataFd}.





-spec append(pulsedb:tick(), pulsedb:db()) -> pulsedb:db().
append(_, #db{mode = read, path = Path}) ->
  error({need_to_reopen_pulsedb_for_append,Path});

append({_Name, UTC, _Value, _Tags} = Tick, #db{config_fd = undefined, date = undefined} = DB) ->
  append(Tick, open0(DB#db{date = pulsedb_time:date_path(UTC)}, append));

append(Tick, #db{mode = append} = DB) ->
  {ok, DB1} = append0(Tick, DB),
  {ok, DB1}.



append0({Name, UTC, Value, Tags}, #db{} = DB) ->
  {ok, SourceId, DB1} = find_or_open_source(Name, Tags, DB),
  {ok, DB2} = append_data(SourceId, UTC, Value, DB1),
  {ok, DB2}.


find_or_open_source(Name, Tags, #db{sources = S} = DB) ->
  {SourceName, DB1} = source_name(Name, Tags, DB),
  case lists:keyfind(SourceName, #source.name, S) of
    false -> append_new_source(SourceName, Name, Tags, DB1);
    #source{id = SourceId} -> {ok, SourceId, DB1}
  end.


source_name(Name, Tags, #db{cached_source_names = SourceNames} = DB) ->
  case lists:keyfind({Name,Tags}, 1, SourceNames) of
    {_, SourceName} -> 
      {SourceName, DB};
    false ->
      SourceName = iolist_to_binary([Name, [[":",atom_to_binary(K,latin1),"=",V] ||  {K,V} <- lists:sort(Tags)]]),
      {SourceName, DB#db{cached_source_names = [{{Name,Tags},SourceName}|SourceNames]}}
  end.



append_new_source(SourceName, Name, Tags, #db{sources = Sources, config_fd = ConfigFd} = DB) ->
  Begin = case Sources of
    [] -> 0;
    _ -> 
      #source{end_of_block = E} = lists:last(Sources),
      E
  end,

  {ok, ConfigPos} = file:position(ConfigFd, eof),
  {ok, TicksPerHour} = application:get_env(pulsedb, ticks_per_hour),
  EOF = Begin + 25*TicksPerHour*(4 + 4),
  Source = #source{id = length(Sources), name = SourceName,
    original_name = Name, original_tags = Tags,
    start_of_block = Begin, end_of_block = EOF,
    data_offset = Begin, data_offset_ptr = ConfigPos + 3},
  Bin = encode_config(Source),
  ok = file:pwrite(ConfigFd, ConfigPos, Bin),
  {ok, Source#source.id, DB#db{sources = Sources ++ [Source]}}.


-define(CONFIG_SOURCE, 2).

encode_config(#source{name = Name, data_offset = Offset, start_of_block = Start, end_of_block = End}) ->
  L = size(Name),
  Conf = <<Offset:32, Start:32, End:32, L:16, Name:L/binary>>,
  Size = iolist_size(Conf),
  iolist_to_binary([<<?CONFIG_SOURCE, Size:16>>, Conf]).

-spec decode_config(binary()) -> [source()].

decode_config(Bin) when is_binary(Bin) ->
  decode_config(Bin, 0).

decode_config(<<?CONFIG_SOURCE, Length:16, Source:Length/binary, Rest/binary>>, ConfigOffset) ->
  <<Offset:32, Start:32, End:32, L:16, Name:L/binary>> = Source,
  DataOffsetPtr = ConfigOffset + 3,
  [OriginalName|Tags] = binary:split(Name, <<":">>, [global]),
  OriginalTags = lists:map(fun(T) ->
    [K,V] = binary:split(T, <<"=">>),
    {binary_to_atom(K,latin1),V}
  end, Tags),
  [#source{name = Name, start_of_block = Start, end_of_block = End, data_offset = Offset, data_offset_ptr = DataOffsetPtr,
    original_name = OriginalName, original_tags = OriginalTags}
    |decode_config(Rest, ConfigOffset + 1 + 2 + Length)];

decode_config(<<>>, _) ->
  [].





append_data(SourceId, UTC, Value, #db{data_fd = DataFd, config_fd = ConfigFd, sources = Sources} = DB) ->
  #source{data_offset = Offset, data_offset_ptr = ConfigPtr, end_of_block = EOF} = Source =
    lists:keyfind(SourceId,#source.id,Sources),

  Block = <<UTC:32, Value:32>>,
  case iolist_size(Block) + Offset =< EOF of
    true ->
      file:pwrite(DataFd, Offset, Block),
      NewDataOffset = Offset + iolist_size(Block),
      file:pwrite(ConfigFd, ConfigPtr, <<NewDataOffset:32>>),
      Sources1 = lists:keystore(SourceId,#source.id, Sources, Source#source{data_offset = NewDataOffset}),
      {ok, DB#db{sources = Sources1}};
    false ->
      {ok, DB}
  end.





info(#db{sources = Sources}) when is_list(Sources) ->
  Src = [{Name,Tags} || #source{original_name = Name, original_tags = Tags} <- Sources],
  [{sources,Src}];

info(#db{path = Path} = DB) ->
  case last_day(Path) of
    undefined -> [];
    DayPath -> info(DB#db{sources = decode_config(read_file(filename:join(DayPath,config_v3)))})
  end.


last_folder(F, Path, Length) ->
  case prim_file:list_dir(F, Path) of
    {ok, List} ->
      case lists:reverse(lists:sort([Y || Y <- List, length(Y) == Length])) of
        [] -> undefined;
        [Y|_] -> Y
      end;
    _ ->
      undefined
  end.

last_day(Path) ->
  {ok, F} = prim_file:start(),
  Val = try last_day0(F, Path)
  catch
    throw:_ -> undefined
  end,
  prim_file:stop(F),
  Val.

last_day0(F, Path) ->
  (Year = last_folder(F, Path, 4)) =/= undefined orelse throw(undefined),
  (Month = last_folder(F, filename:join(Path,Year), 2)) =/= undefined orelse throw(undefined),
  (Day = last_folder(F, filename:join([Path,Year,Month]), 2)) =/= undefined orelse throw(undefined),
  filename:join([Path,Year,Month,Day]).





-spec read(Name::source_name(), Query::[{atom(),any()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.

read(_Name, _Query, #db{mode = append}) ->
  {error, need_to_reopen_for_read};

read(Name, Query, #db{config_fd = undefined, date = Date} = DB) when Date =/= undefined ->
  case open0(DB, read) of
    #db{config_fd = undefined} = DB1 ->
      {ok, [], DB1};
    #db{} = DB1 ->
      read(Name, Query, DB1)
  end;

read(Name, Query0, #db{sources = Sources, data_fd = DataFd, date = Date, mode = read} = DB) when Date =/= undefined ->
  Query = pulsedb:parse_query(Query0),
  Tags = [{K,V} || {K,V} <- Query, K =/= from andalso K =/= to],
  ReadSources = select_sources(Name, Tags, Sources),

  Ticks1 = lists:flatmap(fun(#source{start_of_block = Start, data_offset = Offset}) ->
    case file:pread(DataFd, Start, Offset - Start) of
      {ok, Bin} ->
        TicksBin1 = [ {UTC,Value} || <<UTC:32, Value:32>> <= Bin],
        TicksBin2 = filter_ticks(TicksBin1, proplists:get_value(from,Query), proplists:get_value(to,Query)),
        Ticks = [{UTC, Value} || {UTC,Value} <- TicksBin2],
        Ticks;
      _ ->
        []
    end
  end, ReadSources),
  Ticks2 = lists:sort(Ticks1),
  Ticks3 = aggregate(Ticks2),
  {ok, Ticks3, DB}.



select_sources(_Name, _Tags, []) ->
  [];
select_sources(Name, Tags, [#source{original_name = Name, original_tags = Tags1} = S|Sources]) ->
  BadTags = [K || {K,V} <- Tags, proplists:get_value(K,Tags1) =/= V],
  case BadTags of
    [] -> [S|select_sources(Name, Tags, Sources)];
    _ -> select_sources(Name, Tags, Sources)
  end;

select_sources(Name, Tags, [_|Sources]) ->
  select_sources(Name, Tags, Sources).


aggregate([{UTC,V1},{UTC,V2}|Ticks]) -> aggregate([{UTC,V1+V2}|Ticks]);
aggregate([{UTC,V}|Ticks]) -> [{UTC,V}|aggregate(Ticks)];
aggregate([]) -> [].



filter_ticks([], _, _) ->
  [];

filter_ticks([{UTC,_}|Ticks], From, To) when From - UTC > 0 ->
  filter_ticks(Ticks, From, To);

filter_ticks([{UTC,_}|_Ticks], _From, To) when UTC - To >= 0 ->
  [];

filter_ticks([Tick|Ticks], From, To) ->
  [Tick|filter_ticks(Ticks, From, To)].




-spec close(pulsedb:db()) -> {pulsedb:db()}.
close(#db{config_fd = C, data_fd = D} = DB) ->
  file:close(C),
  file:close(D),
  {ok, DB#db{config_fd = undefined, data_fd = undefined, 
    date = undefined, sources = undefined}}.








