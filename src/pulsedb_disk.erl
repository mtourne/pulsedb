-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").


-record(source, {
  id :: non_neg_integer(),
  name :: source_name(),
  original_name :: source_name(),
  original_tags :: list(),
  data_offset :: non_neg_integer(),
  data_offset_ptr :: non_neg_integer(),
  start_of_block :: non_neg_integer(),
  end_of_block :: non_neg_integer()
}).

-type source() :: #source{}.



-record(disk_db, {
  storage = pulsedb_disk,
  path :: file:filename(),
  date :: binary() | undefined,
  sources :: [source()],
  cached_source_names = [],

  mode :: undefined | read | append,

  config_fd :: file:fd(),
  data_fd :: file:fd()
}).



-export([open/1, append/2, read/3, close/1]).
-export([info/1]).
-export([delete_older/2]).

-export([metric_name/2, metric_fits_query/2, aggregate/2]).
% -export([write_events/3]).


-spec open(Path::file:filename()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path) when is_list(Path) ->
  open(iolist_to_binary(Path));
open(<<"file://", Path/binary>>) ->
  open(Path);
open(Path) ->
  {ok, #disk_db{path = Path}}.


open0(#disk_db{path = Path, config_fd = undefined, date = Date} = DB, Mode) when Date =/= undefined ->
  case filelib:is_regular(filename:join([Path,Date,config_v3])) of
    true ->
      try open_existing_db(DB#disk_db{mode = Mode})
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~p\n~p\n", [Class, Error, Date, Trace]),
          file:delete(filename:join([Path,Date,config_v3])),
          file:delete(filename:join([Path,Date,data_v3])),
          erlang:raise(Class, Error, Trace)
      end;
    false when Mode == read ->
      DB#disk_db{mode = read};
    false when Mode == append ->
      create_new_db(DB#disk_db{mode = append})
  end.


create_new_db(#disk_db{path = Path, date = Date, mode = append} = DB) when Date =/= undefined ->
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

  DB#disk_db{path = Path, config_fd = ConfigFd, data_fd = DataFd, sources = []}.


read_file(Path) ->
  case file:read_file(Path) of
    {ok, Bin} -> Bin;
    {error, _} -> <<>>
  end.

open_existing_db(#disk_db{path = Path, date = Date, mode = Mode} = DB) when Date =/= undefined, Mode =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v3]),
  DataPath = filename:join([Path, Date, data_v3]),

  DB1 = case DB#disk_db.sources of
    undefined ->
      DB#disk_db{sources = decode_config(read_file(ConfigPath))};
    _ ->
      DB
  end,

  Opts = case Mode of
    append -> [binary,read,write,raw];
    read -> [binary,read,raw]
  end,

  {ok, ConfigFd} = file:open(ConfigPath, Opts),
  {ok, DataFd} = file:open(DataPath, Opts),

  DB1#disk_db{config_fd = ConfigFd, data_fd = DataFd}.





-spec append(pulsedb:tick() | [pulsedb:tick()], pulsedb:db()) -> pulsedb:db().

append([{_,_,_,_} = Tick|Ticks], #disk_db{} = DB) ->
  {ok, DB1} = append(Tick, DB),
  append(Ticks, DB1);

append([], #disk_db{} = DB) ->
  {ok, DB};

append(_, #disk_db{mode = read, path = Path}) ->
  error({need_to_reopen_pulsedb_for_append,Path});

append({Name, UTC, Value, _Tags} = T, #disk_db{}) when not is_binary(Name); not is_integer(UTC); not is_integer(Value) ->
  error({invalid_tick, T});

append({_Name, UTC, _Value, _Tags} = Tick, #disk_db{config_fd = undefined, date = undefined} = DB) ->
  append(Tick, open0(DB#disk_db{date = pulsedb_time:date_path(UTC)}, append));

append({_, UTC, _, _} = Tick, #disk_db{mode = append, date = Date, path = Path} = DB) ->
  UTCDate = pulsedb_time:date_path(UTC),
  {ok, DB1} = if
    Date == undefined orelse UTCDate == Date -> 
      try append0(Tick, DB)
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~s\n~p\n", [Class, Error, UTCDate, Trace]),
          file:delete(filename:join([Path,UTCDate,config_v3])),
          file:delete(filename:join([Path,UTCDate,data_v3])),
          erlang:raise(Class, Error, Trace)
      end;
    true ->
      {ok, DB_} = close(DB),
      append(Tick, DB_)
  end,
  {ok, DB1}.



append0({Name, UTC, Value, Tags}, #disk_db{} = DB) ->
  {ok, SourceId, DB1} = find_or_open_source(Name, Tags, DB),
  {ok, DB2} = append_data(SourceId, UTC, Value, DB1),
  {ok, DB2}.


find_or_open_source(Name, Tags, #disk_db{sources = S} = DB) ->
  {SourceName, DB1} = source_name(Name, Tags, DB),
  case lists:keyfind(SourceName, #source.name, S) of
    false -> append_new_source(SourceName, Name, Tags, DB1);
    #source{id = SourceId} -> {ok, SourceId, DB1}
  end.


source_name(Name, Tags, #disk_db{cached_source_names = SourceNames} = DB) ->
  case lists:keyfind({Name,Tags}, 1, SourceNames) of
    {_, SourceName} -> 
      {SourceName, DB};
    false ->
      SourceName = metric_name(Name, Tags),
      {SourceName, DB#disk_db{cached_source_names = [{{Name,Tags},SourceName}|SourceNames]}}
  end.



append_new_source(SourceName, Name, Tags, #disk_db{sources = Sources, config_fd = ConfigFd} = DB) ->
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
  {ok, Source#source.id, DB#disk_db{sources = Sources ++ [Source]}}.


-define(CONFIG_SOURCE, 2).

encode_config(#source{name = Name, data_offset = Offset, start_of_block = Start, end_of_block = End}) ->
  L = size(Name),
  Conf = <<Offset:32, Start:32, End:32, L:16, Name:L/binary>>,
  Size = iolist_size(Conf),
  iolist_to_binary([<<?CONFIG_SOURCE, Size:16>>, Conf]).

-spec decode_config(binary()) -> [source()].

decode_config(Bin) when is_binary(Bin) ->
  decode_config(Bin, 0, 0).

decode_config(<<?CONFIG_SOURCE, Length:16, Source:Length/binary, Rest/binary>>, ConfigOffset, Id) ->
  <<Offset:32, Start:32, End:32, L:16, Name:L/binary>> = Source,
  DataOffsetPtr = ConfigOffset + 3,
  [OriginalName|Tags] = binary:split(Name, <<":">>, [global]),
  OriginalTags = lists:map(fun(T) ->
    [K,V] = binary:split(T, <<"=">>),
    {K,V}
  end, Tags),
  [#source{id = Id, name = Name, start_of_block = Start, end_of_block = End, data_offset = Offset, data_offset_ptr = DataOffsetPtr,
    original_name = OriginalName, original_tags = OriginalTags}
    |decode_config(Rest, ConfigOffset + 1 + 2 + Length, Id + 1)];

decode_config(<<>>, _, _) ->
  [].



metric_name(Name, Tags) ->
 iolist_to_binary([Name, [[":",K,"=",V] ||  {K,V} <- lists:sort(Tags)]]).


append_data(SourceId, UTC, Value, #disk_db{data_fd = DataFd, config_fd = ConfigFd, sources = Sources} = DB) ->
  #source{data_offset = Offset, data_offset_ptr = ConfigPtr, end_of_block = EOF} = Source =
    lists:keyfind(SourceId,#source.id,Sources),

  Block = <<UTC:32, Value:32>>,
  case iolist_size(Block) + Offset =< EOF of
    true ->
      file:pwrite(DataFd, Offset, Block),
      NewDataOffset = Offset + iolist_size(Block),
      file:pwrite(ConfigFd, ConfigPtr, <<NewDataOffset:32>>),
      Sources1 = lists:keystore(SourceId,#source.id, Sources, Source#source{data_offset = NewDataOffset}),
      {ok, DB#disk_db{sources = Sources1}};
    false ->
      {ok, DB}
  end.





info(#disk_db{sources = Sources}) when is_list(Sources) ->
  Src = [{Name,Tags} || #source{original_name = Name, original_tags = Tags} <- Sources],
  [{sources,Src}];

info(#disk_db{path = Path} = DB) ->
  case last_day(Path) of
    undefined -> [];
    DayPath -> info(DB#disk_db{sources = decode_config(read_file(filename:join(DayPath,config_v3)))})
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





-spec read(Name::source_name(), Query::[{binary(),binary()}], pulsedb:db()) -> {ok, [tick()], pulsedb:db()} | {error, Reason::any()}.

% read(_Name, _Query, #disk_db{mode = append}) ->
%   {error, need_to_reopen_for_read};

read(Name, Query, #disk_db{path = Path, date = Date} = DB) ->
  RequiredDates = required_dates(Query),
  case load_ticks(RequiredDates, Name, Query, #disk_db{path = Path, date = Date}) of
    {ok, Ticks, DB1} ->
      close(DB1),
      {ok, Ticks, DB};
    {error, _} = Error ->
      Error
  end.



required_dates(Query) ->
  {from,From} = lists:keyfind(from,1,Query),
  {to,To} = lists:keyfind(to,1,Query),
  [pulsedb_time:date_path(T) || T <- lists:seq(From,To,86400)].


load_ticks([], _Name, _Query, DB) ->
  {ok, [], DB};

load_ticks([Date|Dates], Name, Query, #disk_db{} = DB) ->
  case read0(Name, Query, DB#disk_db{date = Date}) of
    {ok, Ticks1, DB1} ->
      {ok, DB2} = close(DB1),
      case load_ticks(Dates, Name, Query, DB2) of
        {ok, Ticks2, DB3} ->
          {ok, Ticks1++Ticks2, DB3};
        {error, _} = Error ->
          Error
      end;        
    {error, _} = Error ->
      Error
  end.





read0(Name, Query, #disk_db{config_fd = undefined, date = Date} = DB) when Date =/= undefined ->
  case open0(DB, read) of
    #disk_db{config_fd = undefined} = DB1 ->
      {ok, [], DB1};
    #disk_db{} = DB1 ->
      read0(Name, Query, DB1)
  end;

read0(Name, Query, #disk_db{sources = Sources, data_fd = DataFd, date = Date, mode = read} = DB) when Date =/= undefined ->

  Tags = [{K,V} || {K,V} <- Query, is_binary(K)],

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
  Ticks3 = aggregate(proplists:get_value(aggregator,Query), Ticks2),
  {ok, Ticks3, DB}.


metric_fits_query(Query, Tags) ->
  [] == [K || {K,V} <- Query, proplists:get_value(K,Tags) =/= V].



select_sources(_Name, _Tags, []) ->
  [];
select_sources(Name, Tags, [#source{original_name = Name, original_tags = Tags1} = S|Sources]) ->
  case metric_fits_query(Tags, Tags1) of
    true -> [S|select_sources(Name, Tags, Sources)];
    _ -> select_sources(Name, Tags, Sources)
  end;

select_sources(Name, Tags, [_|Sources]) ->
  select_sources(Name, Tags, Sources).


aggregate(Aggegator, Ticks) ->
  Agg = case Aggegator of
    undefined -> fun sum/1;
    <<"sum">> -> fun sum/1;
    <<"avg">> -> fun avg/1;
    <<"max">> -> fun max/1;
    Else -> error({unknown_aggregator,Else})
  end,
  aggregate(Agg, Ticks, undefined, []).


aggregate(_Agg, [], undefined, []) ->
  [];
aggregate(Agg, [], UTC, Acc) -> [{UTC,Agg(Acc)}];
aggregate(Agg, [{UTC,V1}|Ticks], undefined, []) -> aggregate(Agg, Ticks, UTC, [V1]);
aggregate(Agg, [{UTC,V1}|Ticks], UTC, Acc) -> aggregate(Agg, Ticks, UTC, [V1|Acc]);
aggregate(Agg, [{UTC2,V1}|Ticks], UTC1, Acc) -> 
  [{UTC1,Agg(Acc)}|aggregate(Agg, Ticks, UTC2, [V1])].


sum(Acc) -> lists:sum(Acc).

avg([]) -> 0;
avg(Acc) -> lists:sum(Acc) div length(Acc).

max([]) -> 0;
max(Acc) -> lists:max(Acc).



filter_ticks([], _, _) ->
  [];

filter_ticks([{UTC,_}|Ticks], From, To) when From - UTC > 0 ->
  filter_ticks(Ticks, From, To);

filter_ticks([{UTC,_}|_Ticks], _From, To) when UTC - To >= 0 ->
  [];

filter_ticks([Tick|Ticks], From, To) ->
  [Tick|filter_ticks(Ticks, From, To)].




-spec close(pulsedb:db()) -> {pulsedb:db()}.
close(#disk_db{config_fd = C, data_fd = D} = DB) ->
  file:close(C),
  file:close(D),
  {ok, DB#disk_db{config_fd = undefined, data_fd = undefined, 
    date = undefined, sources = undefined}}.




delete_older(Time, #disk_db{path = Path} = DB) ->
  {Now, _} = pulsedb:current_second(),
  GoodDate = binary_to_list(pulsedb_time:date_path(Now - Time)),
  Dates = [Date || Date <- filelib:wildcard("*/*/*", binary_to_list(Path)), Date < GoodDate],

  [begin
    file:delete(filename:join([Path,Date,config_v3])),
    file:delete(filename:join([Path,Date,data_v3])),
    file:del_dir(filename:join(Path,Date)),
    file:del_dir(filename:join(Path,filename:dirname(Date))),
    file:del_dir(filename:join(Path,filename:dirname(filename:dirname(Date)))),
    ok
  end || Date <- Dates],

  {ok, DB}.





