-module(pulsedb_disk).
-author('Max Lapshin <max@maxidoors.ru>').

-include("pulsedb.hrl").


-record(source, {
  id :: non_neg_integer(),
  name :: source_name(),
  original_name :: source_name(),
  original_tags :: list(),
  offsets_offset :: non_neg_integer(),
  block_offsets = [] :: non_neg_integer()
}).

-type source() :: #source{}.


-record(storage_config, 
 {
  ticks_per_chunk = 3600,
  tick_size = 2,
  chunks_per_metric = 24,
  chunk_bitsize = 13,
  offset_size = 4,
  partition_module = pulsedb_disk
  }).

-type storage_config() :: #storage_config{}.

-record(disk_db, 
 {
  storage = pulsedb_disk,
  path :: file:filename(),
  date :: binary() | undefined,
  sources :: [source()],
  cached_source_names = [],

  mode :: undefined | read | append,

  config_fd :: file:fd(),
  data_fd :: file:fd(),
  
  config = #storage_config{}
}).



-export([open/2, append/2, read/3, close/1, sync/1]).
-export([info/1]).
-export([delete_older/2]).
-export([metric_name/2, metric_fits_query/2]).

-export([required_chunks/4, chunk_start/1, chunk_end/1, required_partitions/3, block_path/1, parse_date/1, tick_offset/3, chunk_number/2]).


-spec open(Path::file:filename(), Options::list()) -> {ok, pulsedb:db()} | {error, Reason::any()}.
open(Path, Options) when is_list(Path) ->
  open(iolist_to_binary(Path), Options);
open(<<"file://", Path/binary>>, Options) ->
  open(Path, Options);
open(Path, _Options) when is_binary(Path) ->
  {ok, #disk_db{path = Path}}.


open0(#disk_db{path = Path, config_fd = undefined, date = Date} = DB, Mode) when Date =/= undefined ->
  case filelib:is_regular(filename:join([Path,Date,config_v4])) of
    true ->
      try open_existing_db(DB#disk_db{mode = Mode})
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~p\n~p\n", [Class, Error, Date, Trace]),
          file:delete(filename:join([Path,Date,config_v4])),
          file:delete(filename:join([Path,Date,data_v4])),
          erlang:raise(Class, Error, Trace)
      end;
    false when Mode == read ->
      DB#disk_db{mode = read};
    false when Mode == append ->
      create_new_db(DB#disk_db{mode = append})
  end.


create_new_db(#disk_db{path = Path, date = Date, mode = append} = DB) when Date =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v4]),
  DataPath = filename:join([Path, Date, data_v4]),

  case filelib:ensure_dir(ConfigPath) of
    ok -> ok;
    {error, Reason1} -> throw({error, {create_path_failed,ConfigPath,Reason1}})
  end,

  Opts = [binary,write,exclusive,raw],

  {ok, ConfigFd} = case file:open(ConfigPath, Opts) of
    {ok, CFile_} -> {ok, CFile_};
    {error, Reason2} -> throw({error,{open_config_failed,ConfigPath,Reason2}})
  end,

  {ok, DataFd} = file:open(DataPath, Opts ++ [{delayed_write, 128000, 5000}]),

  DB#disk_db{path = Path, config_fd = ConfigFd, data_fd = DataFd, sources = []}.


read_file(Path) ->
  case file:read_file(Path) of
    {ok, Bin} -> Bin;
    {error, _} -> <<>>
  end.

open_existing_db(#disk_db{path = Path, date = Date, mode = Mode, config = Config} = DB) when Date =/= undefined, Mode =/= undefined ->
  ConfigPath = filename:join([Path, Date, config_v4]),
  DataPath = filename:join([Path, Date, data_v4]),

  DB1 = case DB#disk_db.sources of
    undefined ->
      DB#disk_db{sources = decode_config(read_file(ConfigPath), Config)};
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




sync(#disk_db{config_fd = ConfigFd, data_fd = DataFd} = DB) ->
  case ConfigFd of
    undefined -> ok;
    _ -> file:sync(ConfigFd)
  end,
  case DataFd of
    undefined -> ok;
    _ -> file:sync(DataFd)
  end,
  {ok, DB}.


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

append({_Name, UTC, _Value, _Tags} = Tick, #disk_db{config_fd = undefined, date = undefined, 
                                                    config = #storage_config{partition_module=Partition}} = DB) ->
  append(Tick, open0(DB#disk_db{date = Partition:block_path(UTC)}, append));

append({_, UTC, _, _} = Tick, #disk_db{mode = append, date = Date, path = Path,
                                       config = #storage_config{partition_module=Partition}} = DB) ->
  UTCDate = Partition:block_path(UTC),
  {ok, DB1} = if
    Date == undefined orelse UTCDate == Date ->
      try append0(Tick, DB)
      catch
        Class:Error ->
          Trace = erlang:get_stacktrace(),
          lager:log(error,[{module,?MODULE},{line,?LINE}],"Error in pulsedb: ~p:~p, need to recover for date: ~s\n~p\n", [Class, Error, UTCDate, Trace]),
          file:delete(filename:join([Path,UTCDate,config_v4])),
          file:delete(filename:join([Path,UTCDate,data_v4])),
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
      CachedSourceNames = if 
        length(SourceNames) > 100 -> [{{Name,Tags},SourceName}] ++ lists:sublist(SourceNames, 90);
        true -> [{{Name,Tags},SourceName}] ++ SourceNames
      end,
      {SourceName, DB#disk_db{cached_source_names = CachedSourceNames}}
  end.



append_new_source(SourceName, Name, Tags, #disk_db{sources = Sources, config_fd = ConfigFd} = DB) ->
  {ok, ConfigPos} = file:position(ConfigFd, eof),
  Source = #source{id = length(Sources), name = SourceName, offsets_offset = ConfigPos + 5 + size(SourceName),
    original_name = Name, original_tags = Tags},
  Bin = encode_config(Source),
  ok = file:pwrite(ConfigFd, ConfigPos, Bin),
  {ok, Source#source.id, DB#disk_db{sources = Sources ++ [Source]}}.


-define(CONFIG_SOURCE, 4).

encode_config(#source{name = Name, block_offsets = Offsets}) ->
  L = size(Name),
  OffsetsBin = pack_offsets(Offsets),
  Conf = [<<L:16, Name:L/binary>>, OffsetsBin],
  Size = iolist_size(Conf),
  iolist_to_binary([<<?CONFIG_SOURCE, Size:16>>, Conf]).


pack_offsets(Offsets) ->
  [<<(proplists:get_value(I, Offsets, -1)):32/signed>> || I <- lists:seq(0,23)].

-spec decode_config(binary(), storage_config()) -> [source()].

decode_config(Bin, #storage_config{}=Config) when is_binary(Bin) ->
  decode_config(Bin, 0, 0, Config).

decode_config(<<?CONFIG_SOURCE, Length:16, Source:Length/binary, Rest/binary>>, ConfigOffset, Id, 
              #storage_config{chunks_per_metric=NChunks, offset_size=OSize}=Config) ->
  OffsetsSize = NChunks * OSize,
  OffsetSize = OSize * 8,
  <<L:16, Name:L/binary, OffsetsBin:OffsetsSize/binary>> = Source,
  Offsets =[ {I,O} || {I,O} <- lists:zip(lists:seq(0,NChunks-1), [O || <<O:OffsetSize/signed>> <= OffsetsBin]), O >= 0],

  [OriginalName|Tags] = binary:split(Name, <<":">>, [global]),
  OriginalTags = lists:map(fun(T) ->
    [K,V] = binary:split(T, <<"=">>),
    {K,V}
  end, Tags),
  [#source{id = Id, name = Name, block_offsets = Offsets, offsets_offset = ConfigOffset + 1 + 2 + 2 + L,
    original_name = OriginalName, original_tags = OriginalTags}
    |decode_config(Rest, ConfigOffset + 1 + 2 + Length, Id + 1, Config)];

decode_config(<<>>, _, _, _) ->
  [].



metric_name(Name, Tags) ->
 iolist_to_binary([to_b(Name), [[":",to_b(K),"=",to_b(V)] ||  {K,V} <- lists:sort(Tags)]]).

to_b(Atom) when is_atom(Atom) -> atom_to_binary(Atom, latin1);
to_b(Bin) when is_binary(Bin) -> Bin.

append_data(SourceId, UTC, Value, #disk_db{data_fd = DataFd} = DB) ->
  #disk_db{sources = Sources, 
           config = #storage_config{partition_module = Partition}=Config} = DB1 = open_hour_if_required(SourceId, UTC, DB),
  #source{block_offsets = Offsets} = lists:keyfind(SourceId,#source.id,Sources),
  Offset = Partition:tick_offset(UTC, Offsets, Config),
  ok = file:pwrite(DataFd, Offset, pulsedb_data:shift_value(Value)),
  {ok, DB1}.





open_hour_if_required(SourceId, UTC, #disk_db{config_fd = ConfigFd, data_fd = DataFd, sources = Sources, 
                                              config = #storage_config{ticks_per_chunk=NTicks, tick_size=TickSize,
                                                                       chunk_bitsize=ChunkBits, partition_module=Partition}=Config} = DB) ->
  ChunkSize = 1 bsl ChunkBits,
  ChunkTicksSize = TickSize * NTicks,
  ChunkStuffSize = ChunkSize - ChunkTicksSize,
  ChunkMaxInfoSize = ChunkStuffSize - 1,
  ChunkNo = Partition:chunk_number(UTC, Config),
  
  #source{block_offsets = Offsets, name = Name, offsets_offset = O} = Source = lists:keyfind(SourceId,#source.id,Sources),
  case proplists:get_value(ChunkNo, Offsets) of
    Offset when is_number(Offset) andalso Offset >= 0 ->
      DB;
    _ ->
      {ok, DataPos} = file:position(DataFd, eof),
      BlockOffset = case DataPos rem ChunkSize of
        0 ->
          DataPos;
        _ ->
          lager:error("Error with data file for ~p / ~p. Last offset is ~p", [DB#disk_db.path, DB#disk_db.date, DataPos]),
          ((DataPos div ChunkSize) + 1)*ChunkSize
      end,
      AdditionalInfo = case iolist_to_binary([Partition:block_path(UTC)," ", integer_to_list(ChunkNo), " ", Name]) of
        <<Info:ChunkMaxInfoSize/binary, _/binary>> -> <<Info/binary, 0>>;
        Info -> [Info, binary:copy(<<0>>, ChunkStuffSize - size(Info))]
      end,
      ok = file:pwrite(DataFd, BlockOffset + ChunkTicksSize, AdditionalInfo),
      Offsets1 = lists:keystore(ChunkNo, 1, Offsets, {ChunkNo, BlockOffset bsr ChunkBits}),
      ok = file:pwrite(ConfigFd, O, pack_offsets(Offsets1)),

      Sources1 = lists:keystore(SourceId, #source.id, Sources, Source#source{block_offsets = Offsets1}),
      DB#disk_db{sources = Sources1}
  end.






info(#disk_db{sources = Sources}) when is_list(Sources) ->
  Src = lists:sort([{Name,lists:sort(Tags)} || #source{original_name = Name, original_tags = Tags} <- Sources]),
  [{sources,Src}];

info(#disk_db{path = Path, config=Config} = DB) ->
  case last_day(Path) of
    undefined -> [];
    DayPath -> info(DB#disk_db{sources = decode_config(read_file(filename:join(DayPath,config_v4)), Config)})
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

read(Name, Query, #disk_db{path = Path, date = Date, config = #storage_config{partition_module=Partition}=Config} = DB) ->
  {from,From} = lists:keyfind(from,1,Query),
  {to,To} = lists:keyfind(to,1,Query),
  RequiredDates = Partition:required_partitions(From, To, Config),
  case load_ticks(RequiredDates, Name, Query, #disk_db{path = Path, date = Date}) of
    {ok, Ticks, DB1} ->
      close(DB1),
      {ok, Ticks, DB};
    {error, _} = Error ->
      Error
  end.



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

read0(Name, Query, #disk_db{sources = Sources, data_fd = DataFd, date = Date, mode = read, 
                            config = #storage_config{ticks_per_chunk=NTicks, 
                                                     tick_size=TickSize, 
                                                     chunk_bitsize=ChunkBits,
                                                     partition_module=Partition} = Config} = DB) when Date =/= undefined ->
  Tags = [{K,V} || {K,V} <- Query, is_binary(K)],

  ReadSources = select_sources(Name, Tags, Sources),

  DateUTC = Partition:parse_date(Date),
  From = proplists:get_value(from,Query, Partition:chunk_start(DateUTC)),
  To = proplists:get_value(to,Query, Partition:chunk_end(DateUTC)),

  HrsToRead = Partition:required_chunks(From, To, DateUTC, Config),

  Ticks1 = lists:flatmap(fun({H,BlockOffset,Limit}) ->
    Ticks2 = lists:flatmap(fun(#source{block_offsets = Offsets}) ->
      case lists:keyfind(H, 1, Offsets) of
        {H, Offset} ->
          case file:pread(DataFd, Offset bsl ChunkBits + TickSize*BlockOffset, TickSize*Limit) of
            {ok, Bin} ->
              pulsedb_data:unpack_ticks(Bin, DateUTC + H*NTicks + BlockOffset);
            _ ->
              []
          end;
        false ->
          []
      end        
    end, ReadSources),

    Ticks3 = lists:sort(Ticks2),

    Ticks4 = pulsedb_data:aggregate(proplists:get_value(aggregator,Query), Ticks3),
    erlang:garbage_collect(self()),
    Ticks4
  end, HrsToRead),
  
  Ticks5 = pulsedb_data:downsample(proplists:get_value(downsampler,Query), Ticks1),
  {ok, Ticks5, DB}.





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





-spec close(pulsedb:db()) -> {pulsedb:db()}.
close(#disk_db{config_fd = C, data_fd = D} = DB) ->
  file:close(C),
  file:close(D),
  {ok, DB#disk_db{config_fd = undefined, data_fd = undefined,
    date = undefined, sources = undefined}}.




delete_older(Time, #disk_db{path = Path, config = #storage_config{partition_module = Partition}} = DB) ->
  {Now, _} = pulsedb:current_second(),
  GoodDate = binary_to_list(Partition:block_path(Now - Time)),
  Dates = [Date || Date <- filelib:wildcard("*/*/*", binary_to_list(Path)), Date < GoodDate],

  [begin
    file:delete(filename:join([Path,Date,config_v4])),
    file:delete(filename:join([Path,Date,data_v4])),
    file:del_dir(filename:join(Path,Date)),
    file:del_dir(filename:join(Path,filename:dirname(Date))),
    file:del_dir(filename:join(Path,filename:dirname(filename:dirname(Date)))),
    ok
  end || Date <- Dates],

  {ok, DB}.









tick_offset(UTC, Offsets, #storage_config{ticks_per_chunk=NTicks,
                                          chunk_bitsize=ChunkBits,
                                          tick_size = TickSize}=Config) ->
  Hour = chunk_number(UTC, Config),
  {_, BlockOffset} = lists:keyfind(Hour, 1, Offsets),
  Second = UTC rem NTicks,
  (BlockOffset bsl ChunkBits) + Second*TickSize.

chunk_number(UTC, #storage_config{ticks_per_chunk=NTicks,
                                  chunks_per_metric=NChunks}) ->
   (UTC rem (NChunks*NTicks)) div NTicks.


chunk_start(UTC) ->
  pulsedb_time:daystart(UTC).

chunk_end(UTC) ->
  pulsedb_time:daystart(UTC)+86400 - 1.
  

required_chunks(From, To, Date, #storage_config{ticks_per_chunk = NTicks, chunks_per_metric = NChunks}) ->
  F = From div NTicks,
  T = To div NTicks,
  DateStartD = Date div (NTicks * NChunks),
  
  [begin
     Offset = if 
       F == H -> (From - H*NTicks) rem NTicks;
       true -> 0 end,
     
     Length = if
       Offset > 0     -> NTicks - Offset;
       T == F, T == H -> To - From + 1;
       T == H         -> To - H*NTicks + 1;
       true           -> NTicks end,
     
     {H rem NChunks, Offset, Length}
     end || H <- lists:seq(F, T), H div NChunks == DateStartD].


required_partitions(From, To, #storage_config{ticks_per_chunk = NTicks, chunks_per_metric = NChunks}) ->
  MetricTicksPerBlock = NTicks * NChunks,
  [pulsedb_time:date_path(X*MetricTicksPerBlock) 
   || X <- lists:seq(From div MetricTicksPerBlock,To div MetricTicksPerBlock)].


block_path(UTC) ->
  pulsedb_time:date_path(UTC).


parse_date(Date) ->
  pulsedb_time:parse(Date).
